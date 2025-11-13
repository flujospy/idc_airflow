from prefect import task
import pandas as pd

@task
def transformar(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
    if 'monto' in df.columns:
        df['monto_abs'] = df['monto'].abs()
    return df
