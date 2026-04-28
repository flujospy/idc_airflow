import pandas as pd
from sqlalchemy import create_engine
from prefect import task, get_run_logger
from prefect.exceptions import MissingContextError

from config.settings import settings

def _get_logger():
    try:
        return get_run_logger()
    except MissingContextError:
        import logging
        logger = logging.getLogger("excel_load")
        if not logger.handlers:
            handler= logging.StreamHandler()
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger
    

@task(name = "carga un dataframe a sql")
def load_dataframe_to_mysql(
    df: pd.DataFrame,
    table: str,
    if_exists: str= "replace",
)-> int:
    
    logger = _get_logger()

    conn_str = settings.mysql_connection_string or settings.MYSQL_CONN_STR
    if not conn_str:
        raise ValueError("No hay cadena de conexion SQL configurada en settings")
    
    engine = create_engine(conn_str)


    logger.info(
        f"Cargando DataFrame a sql: tabla = '{table}', if_exists= '{if_exists}',filas= {len(df)}"
    )


    with engine.begin() as conn:
        df.to_sql(
            name= table,
            con=conn,
            if_exists=if_exists,
            index=False,
        )

    logger.info(f"Carga completad. filas insertadas: {len(df)}")
    return len(df)