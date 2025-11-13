from prefect import task, get_run_logger
import pandas as pd


@task(retries=2,retry_delay_seconds=5)
def leer_excel(path: str,sheet:str | int | None=0) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Leyendo archivo: {path}(sheet= {sheet})")
    
    if sheet == "ALL":
        data = pd.read_excel(path,sheet_name=None)
        return pd.concat(data.values(),ignore_index=True)
    
    return pd.read_excel(path,sheet_name=sheet)

@task
def normalizar_columna(df: pd.DataFrame)-> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
                      .str.lower()
                      .str.replace(r"\s+", "_",regex=True)
    )
    return df

@task
def limpiar_basico(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all").drop_duplicates()
    return df