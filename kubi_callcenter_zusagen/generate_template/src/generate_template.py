import logging
from typing import Dict
from didas.oracle import get_engine
import pandas as pd
import paramiko
from sqlalchemy import text
import sys

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)
logger.setLevel(logging.WARNING)


class TemplateGenerationJob:
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

    def _get_new_campaigns(self, dienstleister: str) -> pd.DataFrame:
        sql_string = (
            "select distinct decode(PROVIDER_BEZ,'KLARMOBIL','Klarmobil','Freenet') GESELLSCHAFT, to_char(rbw.TEILNAHME_DATUM,'YYYYMM') as  kampagnenmonat, rbw.AKTION as kampagne, cast(null as number) zusage_hauptprodukt, cast(null as number) zusage_huckepack from BI_KAMPAGNE.REP_BKM_WIRTSCHAFTLICHKEITSBETRACHTUNG_mv rbw where 1=1 and trim(replace(rbw.DIENSTLEISTER,'(Klarmobil)','')) = '"
            + dienstleister
            + "' and trunc(rbw.TEILNAHME_DATUM,'MM') = trunc(sysdate + 7,'mm') and ausschoepfung_ziel is not null order by 1,3"
        )
        sql = text(sql_string)
        with self.engine.connect() as connection:
            new_campaigns = pd.read_sql_query(sql=sql, con=connection)

        new_campaigns.columns = new_campaigns.columns.str.upper()
        return new_campaigns

    def _transfer_file_to_sftp(
        self, dienstleister: str, localpath: str, remotepath: str
    ):
        username = self.sftp_zugaenge[dienstleister]["user"]
        password = self.sftp_zugaenge[dienstleister]["pwd"]
        transport = paramiko.Transport((self.sftp_host, self.sftp_port))
        transport.connect(None, username, password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(localpath, remotepath)
        transport.close()

    def run(self):
        for dienstleister in self.sftp_zugaenge:
            try:
                new_campaigns = self._get_new_campaigns(dienstleister)
                # local_path = "/opt/data/bi_adm/personal/canders/ep_zusagen/vorlagen/ep_zusagen.xlsx"
                local_path = self.local_directory + "ep_zusagen.xlsx"
                remote_path = "/in/ep_zusagen/_vorlagen/ep_zusagen.xlsx"
                if len(new_campaigns.index > 0):
                    new_campaigns.to_excel(
                        local_path, sheet_name="ep_zusagen", index=False
                    )
                    self._transfer_file_to_sftp(dienstleister, local_path, remote_path)
                else:
                    logger.warning(
                        f"Keine Kampagnen für Dienstleister {dienstleister} gefunden"
                    )

            except Exception as e:
                logger.warning(
                    f"Für dienstleister {dienstleister} konnte keine Vorlage generiert werden: {str(e)}"
                )
                continue
