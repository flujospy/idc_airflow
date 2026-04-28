


from prefect import flow, task,get_run_logger
from prefect.task_runners import ConcurrentTaskRunner

from datetime import date, timedelta
from config.settings import settings

from flows.elt_AP_api import etl_aportes_retiros_flow
from flows.etl_api_lotes import etl_api_to_mysql_flow
from flows.etl_onedrive_excel_to_mysql import etl_ondedrive_excel_to_mysql




def _get_fecha_rango_ultimos_n_dias(n: int | None = None) -> tuple[str, str]:
    """
    Devuelve (fecha_inicio, fecha_fin) en formato 'YYYY-MM-DD'
    para los últimos N días hasta hoy (incluido).
    """
    dias = n or settings.BATCH_DAYS or 15

    hoy = date.today()
    # Si tus datos llegan solo hasta AYER, cambia a:
    # fecha_fin_date = hoy - timedelta(days=1)
    fecha_fin_date = hoy - timedelta(days=1)
    fecha_inicio_date = fecha_fin_date - timedelta(days=dias)

    return (
        fecha_inicio_date.strftime("%Y-%m-%d"),
        fecha_fin_date.strftime("%Y-%m-%d"),
    )






#           @task(name = "ETL API saldos")
#
#           def run_etl_saldos():
#
#
#               etl_api_to_mysql_flow()
#                etl_aportes_retiros_flow()


@task(name= "ETL Excel maestro clientes")
def run_etl_excel():
    etl_ondedrive_excel_to_mysql()


@task(name = "ETL Aportes", tags=["opcional"])
def run_etl_aportes(
    inicio: str,
    fin: str
):
    etl_aportes_retiros_flow(
        inicio=inicio,
        fin=fin,
    )


@task(name= "ETL Saldos", tags=["opcional"])
def run_elt_saldos(fecha_inicio: str, fecha_fin: str):
    etl_api_to_mysql_flow(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        tabla_destino="tabla_saldos",
        codigo_producto=2,
        endpoint="/General/ConsultarSaldos",
    )
    pass


@flow(
    name = "master_ETL",
    task_runner=ConcurrentTaskRunner()
)

def master_etl_flow():
    "Orquestador que lanza varios ETLs"

    # api saldos
    # api movimentos
    # onedrive excel

    logger = get_run_logger()
    logger.info("Iniciando master_etl_flow (ejecutando 2-3 etls en paralelo)")

    fecha_inicio, fecha_fin = _get_fecha_rango_ultimos_n_dias()
    logger.info(f"Rango para saldos/aportes: {fecha_inicio} → {fecha_fin}")

    r_saldos= run_elt_saldos.submit(fecha_inicio, fecha_fin)
    r_excel = run_etl_excel.submit()
    raportes= run_etl_aportes.submit(fecha_inicio,fecha_fin)

    _ = [r_saldos.result(), r_excel.result(), raportes.result()]

    logger.info("master_etl_flow finalizado. Revisa cada subflow en la UI de Prefect.")