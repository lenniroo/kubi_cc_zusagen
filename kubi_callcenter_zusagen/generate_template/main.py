import os

from src.generate_template import TemplateGenerationJob
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    sftp_zugaenge = {
        "1ACTIV": {
            "user": os.environ.get("SFTP_1ACTIV_USER"),
            "pwd": os.environ.get("SFTP_1ACTIV_PASSWORD"),
        },
        "CELL_IT": {
            "user": os.environ.get("SFTP_CELL_IT_USER"),
            "pwd": os.environ.get("SFTP_CELL_IT_PASSWORD"),
        },
        "KIKXXL": {
            "user": os.environ.get("SFTP_KIKXXL_USER"),
            "pwd": os.environ.get("SFTP_KIKXXL_PASSWORD"),
        },
        "SKH": {
            "user": os.environ.get("SFTP_SKH_USER"),
            "pwd": os.environ.get("SFTP_SKH_PASSWORD"),
        },
        "XACT ESSEN": {
            "user": os.environ.get("SFTP_XACT_USER"),
            "pwd": os.environ.get("SFTP_XACT_PASSWORD"),
        },
    }
    oracle_hosts = {
        str(os.environ.get("ORACLE_HOST1")),
        str(os.environ.get("ORACLE_HOST2")),
    }
    oracle_port = int(os.environ.get("ORACLE_PORT"))
    oracle_service_name = str(os.environ.get("ORACLE_SERVICE_NAME"))
    oracle_user = str(os.environ.get("ORACLE_USER"))
    oracle_pass = str(os.environ.get("ORACLE_PASS"))
    tz = str(os.environ.get("TZ"))
    nls_lang = str(os.environ.get("NLS_LANG"))
    local_directory = str(os.environ.get("LOCAL_DIRECTORY_VORLAGEN"))
    TemplateGenerationJob(
        sftp_zugaenge,
        oracle_hosts,
        oracle_port,
        oracle_service_name,
        oracle_user,
        oracle_pass,
        tz,
        nls_lang,
        local_directory,
    ).run()
