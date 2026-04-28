from prefect import flow,get_run_logger
from prefect.task_runners import ConcurrentTaskRunner

from tasks.extract_excel_maestro import extract_excel_from_onedrive
from tasks.transform_excel import transform_onedrive_excel
from tasks.excel_load import load_dataframe_to_mysql


@flow(
    name = "etl_onedrive_excel_to_mysql",
    task_runner=ConcurrentTaskRunner()
)

def etl_ondedrive_excel_to_mysql():
    logger=get_run_logger()
    logger.info("Iniciando ETL Onedrive local to MySQL")

    df_raw = extract_excel_from_onedrive()
    df_clean = transform_onedrive_excel(df_raw)

    load_dataframe_to_mysql(
        df_clean,
        table= "cliente_maestro",
        if_exists = "replace"
    )
