
from prefect import flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from datetime import date, timedelta

from tasks.extract_ap import  extract_movimientos_batch
from tasks.extract import authenticate_api
from tasks.transform import transform_movimientos
from tasks.load import load_to_mysql


# Helper: divide un rango en ventanas de 15 días
def split_15d(inicio: date, fin: date):
    """
    Divide un rango de fechas en lotes de 15 días.
    Devuelve tuplas (fecha_inicio_lote, fecha_fin_lote).
    """
    cur = inicio
    while cur <= fin:
        ff = min(cur + timedelta(days=14), fin)
        yield cur, ff
        cur = ff + timedelta(days=1)



@flow(
        name="ETL Movimientos (aportes_retiros) 15d",
        task_runner=ConcurrentTaskRunner()
)       


def etl_aportes_retiros_flow(
    inicio: str, 
    fin: str, 
    endpoint:str ="/General/ConsultarMovimientos",
    tabla_destino: str = "movimiento"
) -> int:
    


    logger = get_run_logger()
    logger.info(f"[APORTES/RETIROS] Ejecutando del {inicio} al {fin}")

    headers = authenticate_api()

    d_ini = date.fromisoformat(inicio)
    d_fin = date.fromisoformat(fin)

    total = 0 #numero de registros cargados.

    for fi, ff in split_15d(d_ini,d_fin):
        logger.info(f"[MOV_ETL] Lote {fi}->{ff}")

        df_raw = extract_movimientos_batch(
            fecha_inicio=fi.isoformat(),
            fecha_final=ff.isoformat(),
            headers= headers,
            endpoint=endpoint,

        )

        if df_raw is None or df_raw.empty:
            logger.warning(f"[MOV_ETL] Lote {fi}..{ff} sin datos, se omite.")
            continue

        # --- TRANSFORM ---
        df_tr = transform_movimientos(df_raw)

        if df_tr is None or df_tr.empty:
            logger.warning(f"[MOV_ETL] Lote {fi}..{ff} sin datos luego de transformar, se omite.")
            continue

        # --- LOAD ---
        filas = load_to_mysql(
            df=df_tr,
            table=tabla_destino,
            fecha_inicio=fi.isoformat(),
            fecha_fin=ff.isoformat(),
            date_column="fecha",  
        )

        total += filas
        logger.info(f"[MOV_ETL] Lote {fi}..{ff} -> {filas} filas cargadas (acumulado={total})")

    logger.info(f"[MOV_ETL] Cantidad TOTAL de registros insertados: {total}")
    return total










