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
    df = df.copy()
    df = df.dropna(how="all")

    columnas_a_limpiar = ["codigoPersona","cod_persona","codigo"]

    for col in columnas_a_limpiar:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: x.strip() if isinstance(x,str) else x 
            )

    df = df.drop_duplicates()
    return df



@task
def limpiar_codigo_persona(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    df = df.copy()

    posibles_columnas = ["codigopersona", "codigo_persona", "codigoPersona", "codigo"]

    columna_encontrada = None
    for col in posibles_columnas:
        if col in df.columns:
            columna_encontrada = col
            break

    if columna_encontrada is None:
        logger.warning("No se encontró columna de código persona para limpiar.")
        return df

    antes = df[columna_encontrada].copy()

    def limpiar_valor(x):
        if not isinstance(x, str):
            return x

        x = x.replace("\t", "")
        x = x.replace("\n", "")
        x = x.replace("\r", "")
        x = x.replace("\xa0", "")
        x = x.strip()

        return x

    df[columna_encontrada] = df[columna_encontrada].apply(limpiar_valor)

    cambios = (antes != df[columna_encontrada]).fillna(False).sum()
    logger.info(f"Columna limpia: {columna_encontrada}")
    logger.info(f"Registros ajustados: {cambios}")

    return df

