from prefect import task, get_run_logger
from datetime import datetime
from typing import Optional
import uuid
from sqlalchemy import text
from config.connections import mysql_connection
from config.settings import settings
from config.settings import settings


@task(name="Iniciar registro de ejecución")

def _q_table(table_name: str) -> str:
    """Nombre cualificado con schema si aplica."""
    return f"{settings.ETL_CONTROL_SCHEMA}.{table_name}" if settings.ETL_CONTROL_SCHEMA else table_name

def start_etl_run(entity_name: str, fecha_inicio: str, fecha_fin: str) -> str:
    """
    Registra el inicio de una ejecución de ETL
    
    Returns:
        run_id: ID único de la ejecución
    """
    logger = get_run_logger()
    run_id = str(uuid.uuid4())
    
    query = text(f"""
        INSERT INTO {settings.ETL_LOGS_TABLE} 
        (run_id, entity_name, start_time, end_time, status, rows_processed, error_msg)
        VALUES 
        (:run_id, :entity_name, :start_time, NULL, 'RUNNING', 0, NULL)
    """)
    
    with mysql_connection.engine.connect() as conn:
        conn.execute(query, {
            "run_id": run_id,
            "entity_name": entity_name,
            "start_time": datetime.now()
        })
        conn.commit()
    
    logger.info(f" Ejecución iniciada - Run ID: {run_id}")
    return run_id


@task(name="Finalizar registro de ejecución")
def end_etl_run(
    run_id: str,
    entity_name: str,
    status: str,
    rows_processed: int,
    error_msg: Optional[str] = None,
    fecha_inicio: str = None,
    fecha_fin: str = None
):
    """
    Registra el fin de una ejecución y actualiza el control
    
    Args:
        run_id: ID de la ejecución
        entity_name: Nombre de la entidad
        status: 'SUCCESS', 'FAILED', 'PARTIAL'
        rows_processed: Número de registros procesados
        error_msg: Mensaje de error (si aplica)
        fecha_inicio: Fecha inicial del rango procesado
        fecha_fin: Fecha final del rango procesado
    """
    logger = get_run_logger()
    end_time = datetime.now()
    
    # 1. Actualizar log de ejecución
    update_log_query = text(f"""
        UPDATE {settings.ETL_LOGS_TABLE}
        SET end_time = :end_time,
            status = :status,
            rows_processed = :rows_processed,
            error_msg = :error_msg
        WHERE run_id = :run_id
    """)
    
    with mysql_connection.engine.connect() as conn:
        conn.execute(update_log_query, {
            "run_id": run_id,
            "end_time": end_time,
            "status": status,
            "rows_processed": rows_processed,
            "error_msg": error_msg
        })
        conn.commit()
    
    # 2. Actualizar tabla de control
    last_since = f"{fecha_inicio} to {fecha_fin}" if fecha_inicio and fecha_fin else "N/A"
    
    # Verificar si existe el registro
    check_query = text(f"""
        SELECT COUNT(*) as count FROM {settings.ETL_CONTROL_TABLE}
        WHERE entity_name = :entity_name
    """)
    
    with mysql_connection.engine.connect() as conn:
        result = conn.execute(check_query, {"entity_name": entity_name})
        exists = result.fetchone()[0] > 0
        
        if exists:
            # Actualizar registro existente
            update_control_query = text(f"""
                UPDATE {settings.ETL_CONTROL_TABLE}
                SET last_run = :last_run,
                    last_since = :last_since,
                    last_status = :last_status,
                    last_rows_processed = :last_rows_processed
                WHERE entity_name = :entity_name
            """)
            conn.execute(update_control_query, {
                "entity_name": entity_name,
                "last_run": end_time,
                "last_since": last_since,
                "last_status": status,
                "last_rows_processed": rows_processed
            })
        else:
            # Insertar nuevo registro
            insert_control_query = text(f"""
                INSERT INTO {settings.ETL_CONTROL_TABLE}
                (entity_name, last_run, last_since, last_status, last_rows_processed)
                VALUES
                (:entity_name, :last_run, :last_since, :last_status, :last_rows_processed)
            """)
            conn.execute(insert_control_query, {
                "entity_name": entity_name,
                "last_run": end_time,
                "last_since": last_since,
                "last_status": status,
                "last_rows_processed": rows_processed
            })
        
        conn.commit()
    
    logger.info(f" Ejecución finalizada - Status: {status}, Registros: {rows_processed}")


@task(name="Obtener última ejecución exitosa")
def get_last_successful_run(entity_name: str) -> Optional[dict]:
    """
    Obtiene información de la última ejecución exitosa
    
    Returns:
        Dict con información de última ejecución o None si no existe
    """
    logger = get_run_logger()
    
    query = text(f"""
        SELECT 
            entity_name,
            last_run,
            last_since,
            last_status,
            last_rows_processed
        FROM {settings.ETL_CONTROL_TABLE}
        WHERE entity_name = :entity_name
    """)
    
    with mysql_connection.engine.connect() as conn:
        result = conn.execute(query, {"entity_name": entity_name})
        row = result.fetchone()
        
        if row:
            info = {
                "entity_name": row[0],
                "last_run": row[1],
                "last_since": row[2],
                "last_status": row[3],
                "last_rows_processed": row[4]
            }
            logger.info(f"Última ejecución: {info['last_run']} - Status: {info['last_status']}")
            return info
        
        logger.info("No se encontraron ejecuciones previas")
        return None


@task(name="Registrar log de lote")
def log_batch_processing(
    run_id: str,
    entity_name: str,
    batch_number: int,
    fecha_inicio: str,
    fecha_fin: str,
    rows_processed: int,
    status: str,
    error_msg: Optional[str] = None
):
    """
    Registra el procesamiento de un lote individual
    Útil para debugging y trazabilidad
    """
    logger = get_run_logger()
    
    batch_run_id = f"{run_id}_batch_{batch_number}"
    
    query = text(f"""
        INSERT INTO {settings.ETL_LOGS_TABLE}
        (run_id, entity_name, start_time, end_time, status, rows_processed, error_msg)
        VALUES
        (:run_id, :entity_name, :start_time, :end_time, :status, :rows_processed, :error_msg)
    """)
    
    with mysql_connection.engine.connect() as conn:
        conn.execute(query, {
            "run_id": batch_run_id,
            "entity_name": f"{entity_name}_batch",
            "start_time": datetime.now(),
            "end_time": datetime.now(),
            "status": status,
            "rows_processed": rows_processed,
            "error_msg": error_msg
        })
        conn.commit()
    
    logger.info(f"Lote {batch_number} registrado: {status} - {rows_processed} rows")
