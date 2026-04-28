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

# transformacion  Aportes y Retiros

@task(name = "Transformacion movimientos (aportes/retiros)",retries=0)
def transform_movimientos(df:pd.DataFrame)->pd.DataFrame:

    if df.empty:
        return df
    
    rename_map = {
        "fechaMovimiento": "fecha_movimiento",
        "numeroMovimiento": "numero_movimiento",
        "codigoPersona": "codigo_persona",
        "nombreCliente": "nombre_cliente",
        "hechoPor": "hecho_por",
        "tipoObservaciones": "tipo_observaciones",
        "valorParticipacion": "valor_participacion",
        "participacionesRestantes": "participaciones_restantes",
        "participaciones": "participaciones",
        "monto": "monto",
        "saldoAnterior": "saldo_anterior",
        "saldoResultante": "saldo_resultante",
        "saldoParticipacionesAnterior": "saldo_participaciones_anterior",
        "tipoMovimiento": "tipo_movimiento",
        "producto": "producto",
        "fechaRegistro": "fecha_registro",
        "codigoMovimiento": "codigo_movimiento",
        "codigoCuenta": "codigo_cuenta",
        "numeroCuenta": "numero_cuenta",
    }

##Convierte nombres de la API (camelCase) a convención de BD (snake_case)
##Usa comprensión de diccionario con if k in df.columns para seguridad:
 
    df = df.rename(columns={
        k: v for k, v in rename_map.items() if k in df.columns
    })


#Convierte strings de fecha → objetos datetime
# errors="coerce": Fechas inválidas se convierten a NaT (Not a Time) en lugar de fallar
    for col in ["fecha_movimiento", "fecha_registro"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors = "coerce")


    if "fecha_movimiento" in df.columns:
        df["fecha"] = df["fecha_movimiento"].dt.date

    # Campos numéricos → tipo numérico
    #Convierte strings numéricos → tipos float64 o int64
    #errors="coerce": Valores no numéricos → NaN (no falla)

    numeric_cols = [
        "numero_movimiento",
        "codigo_cuenta",
        "numero_cuenta",
        "monto",
        "saldo_anterior",
        "saldo_resultante",
        "saldo_participaciones_anterior",
        "participaciones",
        "participaciones_restantes",
        "valor_participacion",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col]= pd.to_numeric(df[col],errors="coerce")


    
    if "id_movimiento" not in df.columns:
        def _build_id(row): 

            cod_mov = str(row.get("codigo_movimiento","") or "").strip()
            cod_cta = str(row.get("codigo_cuenta","")or "").strip()

            fecha=row.get("fecha_movimiento")

            #convierte la fecha en iso
            if pd.notna(fecha):
                fecha_str = pd.to_datetime(fecha).isoformat()
            else:
                fecha_str="" 
            
            return f"{cod_mov}-{cod_cta}-{fecha_str}"
        

        df["id_movimiento"] = df.apply(_build_id,axis=1)

    return df




