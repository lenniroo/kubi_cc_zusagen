import hashlib
import logging
from typing import Dict
from didas.oracle import get_engine
import paramiko
import sys
from datetime import datetime
import os
import pandas as pd

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class GetEpZusagen:
    def __init__(
        self,
        sftp_zugaenge: Dict,
        oracle_hosts: str,
        oracle_port: int,
        oracle_service_name: str,
        oracle_user: str,
        oracle_pass: str,
        tz: str,
        nls_lang: str,
        local_directory: str,
    ) -> None:
        self.engine = get_engine(
            oracle_username=oracle_user,
            oracle_password=oracle_pass,
            oracle_hosts=oracle_hosts,
            oracle_port=oracle_port,
            oracle_servicename=oracle_service_name,
        )
        self.sftp_zugaenge = sftp_zugaenge
        self.sftp_host = "ftp.mobilcom.de"
        self.sftp_port = 22
        self.local_directory = local_directory

    def _get_file_list_from_sftp(self, sftp) -> list:
        files = sftp.listdir(path="/in/ep_zusagen")
        files = list(filter(lambda file: "." in file, files))
        logger.info(f"Dateien: {files}")
        return files

    def _establish_sftp_connection(self, dienstleister):
        username = self.sftp_zugaenge[dienstleister]["user"]
        password = self.sftp_zugaenge[dienstleister]["pwd"]
        transport = paramiko.Transport((self.sftp_host, self.sftp_port))
        transport.connect(None, username, password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return transport, sftp

    def _get_file_from_sftp(self, dienstleister: str, file: str, sftp) -> str:
        localpath = (
            self.local_directory
            # "/opt/data/bi_adm/personal/canders/ep_zusagen/"
            + str(dienstleister)
            + "_"
            + datetime.now().strftime("%Y%m%d%H%M%S")
            + "_"
            + file
        )
        remotepath = "/in/ep_zusagen/" + file
        sftp.get(remotepath, localpath)
        return localpath, remotepath

    def _write_data_to_database(
        self, data: pd.DataFrame, dienstleister: str, filename: str
    ):
        data = data[~data["GESELLSCHAFT"].isnull()].copy()
        data = data[~data["KAMPAGNE"].isnull()].copy()
        data = data[~data["KAMPAGNENMONAT"].isnull()].copy()
        data["DIENSTLEISTER"] = dienstleister
        data["FILENAME"] = filename
        data["KAMPAGNE"] = data["KAMPAGNE"].apply(str.strip)
        data["GESELLSCHAFT"] = data["GESELLSCHAFT"].apply(str.upper)
        data["INSERT_TS"] = datetime.today()
        data[
            "INSERT_USER"
        ] = "https://github.com/freenet-datascience/outbound-callcenter-zusagen"
        data["HASH_DF"] = data.apply(
            lambda x: hashlib.md5(
                (
                    x["DIENSTLEISTER"]
                    + x["GESELLSCHAFT"]
                    + x["KAMPAGNE"]
                    + str(x["KAMPAGNENMONAT"])
                    + str(x["ZUSAGE_HAUPTPRODUKT"])
                    + str(x["ZUSAGE_HUCKEPACK"])
                ).encode()
            ).hexdigest(),
            axis=1,
            result_type="expand",
        )
        if len(data.index) > 0:
            data.to_sql(
                name="rep_outbound_sales_zusagen",
                schema="BI_KAMPAGNE",
                con=self.engine,
                if_exists="append",
                index=False,
            )

    @staticmethod
    def _get_data_from_file(localpath: str) -> pd.DataFrame:
        file_name, file_extension = os.path.splitext(localpath)
        if file_extension == ".xlsx":
            data = pd.read_excel(localpath)
        elif file_extension == ".csv":
            data = pd.read_csv(localpath, sep=";")
        else:
            logger.warning("unbekanntes Dateiformat: ", file_extension)

        data.columns = data.columns.str.upper()
        data.dropna(
            axis="index",
            subset=["ZUSAGE_HAUPTPRODUKT", "ZUSAGE_HUCKEPACK"],
            inplace=True,
            how="all",
        )

        return data

    @staticmethod
    def _remove_remote_file(remotepath: str, sftp):
        sftp.remove(remotepath)

    def run(self):
        for dienstleister in self.sftp_zugaenge:
            logger.info(f"Dienstleister: {dienstleister}")

            transport, sftp = self._establish_sftp_connection(dienstleister)
            files = self._get_file_list_from_sftp(sftp)

            for file in files:
                try:
                    localpath, remotepath = self._get_file_from_sftp(
                        dienstleister, file, sftp
                    )
                    data = self._get_data_from_file(localpath)

                    if len(data.index) > 0:
                        self._write_data_to_database(data, dienstleister, file)
                except Exception as e:
                    logger.error(
                        f"Für Dienstleister {dienstleister} konnten keine Daten eingelesen werden: {str(e)}"
                    )
                    raise

                self._remove_remote_file(remotepath, sftp)

            transport.close()
