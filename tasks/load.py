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
    date_column: str = DATE_COLUMN,
    
) -> int:
    """
    DELETE + LOAD por rango [fecha_inicio, fecha_fin].
    Usa una sola transacción y parámetros enlazados (seguro).
    """
    logger = get_run_logger()
    n = int(len(df) if df is not None else 0)

    logger.info(
        f"[LOAD] Recibidos {n} registros para {table} "
        f"(rango {fecha_inicio}..{fecha_fin})"
    )



    engine = mysql_connection.engine


    with engine.begin() as conn:
        delete_sql = text(
            f"""
            DELETE FROM `{table}`
            WHERE `{date_column}` BETWEEN :start AND :end
            """
        )
        result = conn.execute(
            delete_sql,
            {"start": fecha_inicio, "end": fecha_fin},
        )
        deleted_rows = result.rowcount or 0
        logger.info(
            f"[LOAD] {deleted_rows} registros eliminados en {table} "
            f"(rango {fecha_inicio}..{fecha_fin})"
        )

        # Si el DF viene vacío, aquí terminamos
        if n == 0:
            logger.info(
                f"[LOAD] DataFrame vacío, no se insertan registros en {table}"
            )
            return 0

        # Inserción con to_sql sobre la misma conexión/tx
        df.to_sql(
            name=table,
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

    logger.info(f"[LOAD] {n} registros insertados en {table}")
    return n