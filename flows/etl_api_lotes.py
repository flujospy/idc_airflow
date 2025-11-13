from prefect import flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from datetime import datetime
from typing import Optional
from utils.db import get_engine

from tasks.extract import authenticate_api, extract_from_api_batch
from tasks.transform import clean_and_transform, validate_data
from tasks.load import load_to_mysql
from tasks.notifications import send_notification
from tasks.etl_control import (
    start_etl_run,
    end_etl_run,
    get_last_successful_run,
    log_batch_processing
)
from utils.date_utils import split_date_range
from config.settings import settings

@flow(
    name="ETL API con Control y Logs",
    description="Extrae datos de API en lotes y registra ejecución en tablas de control",
    task_runner=ConcurrentTaskRunner(),
    retries=1,
    retry_delay_seconds=300
)
def etl_api_to_mysql_flow(
    fecha_inicio: str,
    fecha_fin: str,
    entity_name: str = "datos_api",
    tabla_destino: str = "tabla_saldos",
    endpoint: str = "/General/ConsultarSaldos",
    numero_identificacion: Optional[str] = None,
    codigo_producto: Optional[int] = None,
    tipo_producto: str = "M",
    tipo_cliente: str = "M",
):
    """
    Flujo principal de ETL con sistema de control y logs
    
    Args:
        fecha_inicio: Fecha inicial (YYYY-MM-DD)
        fecha_fin: Fecha final (YYYY-MM-DD)
        entity_name: Nombre de la entidad para control
        tabla_destino: Tabla MySQL destino
        endpoint: Endpoint de la API
        numero_identificacion: Número de identificación del cliente
        codigo_producto: Código del producto
        tipo_producto: Tipo de producto (M por defecto)
        tipo_cliente: Tipo de cliente (M por defecto)
    """
    
    logger = get_run_logger()
    start_time = datetime.now()
    
    logger.info("=" * 70)
    logger.info(" INICIANDO ETL API A MYSQL CON CONTROL")
    logger.info("=" * 70)
    logger.info(f" Entidad: {entity_name}")
    logger.info(f" Rango: {fecha_inicio} a {fecha_fin}")
    logger.info(f" Tabla destino: {tabla_destino}")
    logger.info("=" * 70)
    
    # Variables para control - INICIALIZADAS AL INICIO
    run_id = None
    total_registros = 0
    lotes_exitosos = 0
    lotes_fallidos = 0
    lotes = []  # Inicializada aquí
    status = "SUCCESS"
    error_message = None
    duration = 0
    
    headers = authenticate_api()
    try:
        # PASO 1: Verificar última ejecución
        logger.info("\n PASO 1: Verificando última ejecución")
        last_run = get_last_successful_run(entity_name)
        if last_run:
            logger.info(f"  Última ejecución: {last_run['last_run']}")
            logger.info(f"  Status: {last_run['last_status']}")
            logger.info(f"  Registros procesados: {last_run['last_rows_processed']}")
        
        # PASO 2: Iniciar registro de ejecución
        logger.info("\n PASO 2: Iniciando registro de ejecución")
        run_id = start_etl_run(entity_name, fecha_inicio, fecha_fin)
        
        # PASO 3: Autenticación
        logger.info("\n PASO 3: Autenticación")
        token = authenticate_api()
        
        # PASO 4: Dividir rango en lotes
        logger.info(f"\n PASO 4: Dividiendo rango en lotes de {settings.BATCH_DAYS} días")
        lotes = split_date_range(fecha_inicio, fecha_fin, settings.BATCH_DAYS)
        logger.info(f"Total de lotes a procesar: {len(lotes)}")
        
        for i, (inicio, fin) in enumerate(lotes, 1):
            logger.info(f"  Lote {i}: {inicio} a {fin}")
        
        # PASO 5: Procesar cada lote
        logger.info("\n PASO 5: Procesando lotes")
        
        engine = get_engine()

        for i, (inicio_lote, fin_lote) in enumerate(lotes, 1):
            logger.info(f"\n--- Procesando Lote {i}/{len(lotes)} ---")
            batch_rows = 0
            batch_status = "SUCCESS"
            batch_error = None
            
            try:
                # Extracción
                df_raw = extract_from_api_batch(
                    fecha_inicio=inicio_lote,
                    fecha_fin=fin_lote,
                    headers=headers,
                    endpoint=endpoint,
                    numero_identificacion=numero_identificacion,
                    codigo_producto=codigo_producto,
                    tipo_producto=tipo_producto,
                    tipo_cliente=tipo_cliente,
                )
                
                if df_raw.empty:
                    logger.warning(f"Lote {i} vacío, continuando...")
                    log_batch_processing(
                        run_id=run_id,
                        entity_name=entity_name,
                        batch_number=i,
                        fecha_inicio=inicio_lote,
                        fecha_fin=fin_lote,
                        rows_processed=0,
                        status="SUCCESS",
                        error_msg="No data in range"
                    )
                    continue
                
                # Transformación
                df_clean = clean_and_transform(df_raw)
                df_valid = validate_data(df_clean)
                
                # Carga
                batch_rows = load_to_mysql(
                    df=df_valid,
                    table=tabla_destino,
                    fecha_inicio=inicio_lote,
                    fecha_fin=fin_lote,
                    engine=engine
                )
                
                total_registros += batch_rows
                lotes_exitosos += 1
                
                logger.info(f" Lote {i} completado: {batch_rows} registros")
                
            except Exception as e:
                lotes_fallidos += 1
                batch_status = "FAILED"
                batch_error = str(e)
                logger.error(f" Error en lote {i}: {batch_error}")
                
                # Si hay muchos fallos, cambiar status general a PARTIAL
                if lotes_fallidos > len(lotes) * 0.3:  # Más del 30% falló
                    status = "PARTIAL"
            
            finally:
                # Registrar procesamiento del lote
                log_batch_processing(
                    run_id=run_id,
                    entity_name=entity_name,
                    batch_number=i,
                    fecha_inicio=inicio_lote,
                    fecha_fin=fin_lote,
                    rows_processed=batch_rows,
                    status=batch_status,
                    error_msg=batch_error
                )
        
        # Determinar status final
        if lotes_fallidos == len(lotes):
            status = "FAILED"
            error_message = "Todos los lotes fallaron"
        elif lotes_fallidos > 0:
            status = "PARTIAL"
            error_message = f"{lotes_fallidos}/{len(lotes)} lotes fallaron"
        
    except Exception as e:
        status = "FAILED"
        error_message = f"Error crítico: {str(e)}"
        logger.error(f" Error crítico en el ETL: {error_message}")
    
    finally:
        # Calcular duración
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # PASO 6: Finalizar registro
        if run_id:
            logger.info("\n PASO 6: Finalizando registro de ejecución")
            end_etl_run(
                run_id=run_id,
                entity_name=entity_name,
                status=status,
                rows_processed=total_registros,
                error_msg=error_message,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin
            )
        
        # PASO 7: Resumen
        logger.info("\n" + "=" * 70)
        logger.info(" RESUMEN DEL ETL")
        logger.info("=" * 70)
        logger.info(f" Run ID: {run_id}")
        logger.info(f" Entidad: {entity_name}")
        
        # Solo mostrar estadísticas de lotes si existen
        if lotes:
            logger.info(f" Lotes exitosos: {lotes_exitosos}/{len(lotes)}")
            logger.info(f" Lotes fallidos: {lotes_fallidos}/{len(lotes)}")
        else:
            logger.info(f" No se procesaron lotes")
        
        logger.info(f" Total registros: {total_registros}")
        logger.info(f" Duración: {duration:.2f} segundos")
        logger.info(f" Status: {status}")
        if error_message:
            logger.info(f" Error: {error_message}")
        logger.info("=" * 70)
        
        # Notificación
        emoji = "✅" if status == "SUCCESS" else "⚠️" if status == "PARTIAL" else "❌"
        
        # Preparar metadata de forma segura
        metadata = {
            "Run ID": run_id,
            "Entidad": entity_name,
            "Registros procesados": total_registros,
            "Duración": f"{duration:.2f}s",
            "Status": status
        }
        
        # Agregar info de lotes solo si existen
        if lotes:
            metadata["Lotes exitosos"] = f"{lotes_exitosos}/{len(lotes)}"
            metadata["Lotes fallidos"] = f"{lotes_fallidos}/{len(lotes)}"
        
        send_notification(
            mensaje=f"{emoji} ETL {entity_name} completado - Status: {status}",
            metadata=metadata
        )
    
    return {
        "run_id": run_id,
        "status": status,
        "total_registros": total_registros,
        "lotes_exitosos": lotes_exitosos,
        "lotes_fallidos": lotes_fallidos,
        "duracion_segundos": duration
    }