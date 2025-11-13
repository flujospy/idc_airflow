from prefect import task, get_run_logger
import pandas as pd
from config.connections import mysql_connection
from sqlalchemy import text
from prefect.cache_policies import NO_CACHE


DATE_COLUMN = "fecha"

@task(
    name="Cargar datos a MySQL",
    retries=1,
    retry_delay_seconds=30,
    cache_policy=NO_CACHE
)
def load_to_mysql(
    df: pd.DataFrame,
    table: str,
    fecha_inicio: str,
    fecha_fin: str,
    engine
    
) -> int:
    """
    DELETE + LOAD por rango [fecha_inicio, fecha_fin].
    Usa una sola transacción y parámetros enlazados (seguro).
    """
    logger = get_run_logger()
    n = int(len(df) if df is not None else 0)
    logger.info(f"Cargando {n} registros en {table} para rango {fecha_inicio} a {fecha_fin}")
    
    #if df.empty:
    #    logger.warning("DataFrame vacío, no hay datos para cargar")
    #    return 0
    
    if df is None or df.empty:
        logger.info("DataFrame vacío: no se ejecuta Delete ni Load.")


    delete_sql = text(f"""
        DELETE FROM `{table}`
        WHERE `{DATE_COLUMN}` BETWEEN :start AND :end
    """)

    with engine.begin() as conn:
        result = conn.execute(delete_sql, {"start":fecha_inicio,"end":fecha_fin})
        deleted_rows = result.rowcount or 0
        logger.info(f" {deleted_rows} registros eliminados en {table} (rango {fecha_inicio}..{fecha_fin})")

    

    #try:
     #   logger.info(f"Cargando {len(df)} registros a tabla {table_name}")
        
        df.to_sql(
                    name=table,
                    con=conn,           
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )

    logger.info(f" {n} registros cargados en {table}")
    return n