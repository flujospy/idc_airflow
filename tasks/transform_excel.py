

from prefect import task,get_run_logger
import pandas as pd
from prefect.exceptions import MissingContextError

def _get_logger():
    """Logger de Prefect si hay contexto; si no, logger estándar."""
    try:
        return get_run_logger()
    except MissingContextError:
        import logging

        logger = logging.getLogger("transform_onedrive_excel")
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
    name= "Transformar excel OneDrive", retries=0
)

def transform_onedrive_excel(df:pd.DataFrame) -> pd.DataFrame:

    logger = get_run_logger()
    logger.info("Iniciando transformaciones de excel...")

    df = df.copy()

    df.columns = [ str(col).strip().lower().replace(" ","_").replace("ó","o").replace(".","")
               for col in df.columns]

    logger.info(f"Columnas despues de la normalizacion: {list(df.columns)}")


    columnas_necesarias = [
        "cod_persona",
        "nombre",
        "distribucion",
        "ejecutivobusqueda",
        "cartera",
        "tipo_cliente",
        "movimiento",
        "empresa_de_grupo_idc"
    ]

    faltantes = [c for c in columnas_necesarias if c not in df.columns]
    if faltantes:
        raise ValueError(
            f"faltan columnas en el archivo, luego de normalizar {faltantes}"
        )

    df= df[columnas_necesarias].copy()

    mapa_renombres = {
         "ejecutivobusqueda": "gerente",  
         "movimiento": "tipo_movimiento",
         "cod._persona": "cod_persona"
                 }


    if mapa_renombres:
        df=df.rename(columns=mapa_renombres)



    logger.info(f"Transformacion  completada. Filas: {len(df)}, columnas: {list(df.columns)}")
    return df


