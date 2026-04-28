
from pathlib import Path
import pandas as pd
from prefect import task, get_run_logger
from prefect.exceptions import MissingContextError
from config.settings import settings





def _get_logger():
    try:
        return get_run_logger()
    except MissingContextError:
        import logging

        logger = logging.getLogger("extract_excel_from _onedrive")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger
    
@task(

    name = "Descagar Excel desde OneDrive",
    retries= 1 ,
    retry_delay_seconds= 60
)

def extract_excel_from_onedrive() -> pd.DataFrame:
    
    logger = get_run_logger()
    logger.info("Leyendo excel maestro desde OneDrive/local...")

    excel_path = settings.ONEDRIVE_EXCEL_PATH_TABLA_MAESTRA

    if not excel_path:
        raise ValueError(
            "ONEDRIVE_EXCEL_PATH_TABLA_MAESTRA no esta configurado en el .env"
        )

    path_obj = Path(excel_path)
    if not path_obj.exists():
        raise FileNotFoundError(f"no se encontro el archivo excel en: {path_obj}")

    df = pd.read_excel(path_obj, sheet_name="Vista Clientes Midas")
    logger.info(f"Archivo leido con {len(df) }filas y {len(df.columns)} columnas")
    return df


