from prefect import task, get_run_logger
import pandas as pd
from datetime import datetime

@task(name="Limpiar y transformar datos")
def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y transforma los datos extraídos"""
    logger = get_run_logger()
    logger.info(f"Transformando {len(df)} registros")
    
    if df.empty:
        logger.warning("DataFrame vacío, no hay datos para transformar")
        return df
    
    df = df.copy()
    
    # Eliminar duplicados
    initial_count = len(df)
    df = df.drop_duplicates()
    logger.info(f"Duplicados eliminados: {initial_count - len(df)}")
    
    # Normalizar nombres de columnas
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
    
    # Convertir tipos de datos
    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
    
    if 'monto' in df.columns:
        df['monto'] = pd.to_numeric(df['monto'], errors='coerce')
    
    # Agregar metadatos
    df['fecha_carga'] = datetime.now()
    df['fuente'] = 'API'
    
    # Eliminar nulos en columnas críticas
    critical_columns = ['fecha']
    existing_critical = [col for col in critical_columns if col in df.columns]
    
    if existing_critical:
        before = len(df)
        df = df.dropna(subset=existing_critical)
        logger.info(f"Registros con nulos eliminados: {before - len(df)}")
    
    logger.info(f"Transformación completada: {len(df)} registros válidos")
    
    return df


@task(name="Validar calidad de datos")
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Valida la calidad de los datos"""
    logger = get_run_logger()
    
    if df.empty:
        logger.warning("DataFrame vacío después de la validación")
        return df
    
    required_columns = ['fecha']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Columnas requeridas faltantes: {missing_columns}")
    
    #if df['id'].duplicated().any():
     #   duplicates = df['id'].duplicated().sum()
      #  logger.warning(f"Se encontraron {duplicates} IDs duplicados")
    
    logger.info("Validación completada exitosamente")
    
    return df