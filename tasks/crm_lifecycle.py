from prefect import task, get_run_logger
from sqlalchemy import text
from config.connections import mysql_connection


# ── Task 1: validar dimensión ────────────────────────────────
@task(name="validar-estados-no-mapeados", retries=2, retry_delay_seconds=15)
def validar_estados_no_mapeados():
    logger = get_run_logger()
    engine = mysql_connection.engine

    query = """
        SELECT COUNT(*) FROM (
            SELECT DISTINCT h.status_nuevo
            FROM opportunities_status_history h
            LEFT JOIN dim_opportunity_status d
                ON d.status_id = h.status_nuevo
            WHERE d.status_id IS NULL
        ) t
    """
    with engine.connect() as conn:
        cantidad = conn.execute(text(query)).scalar()

    logger.info(f"Estados no mapeados: {cantidad}")
    if cantidad > 0:
        raise ValueError(f"{cantidad} estados sin mapear en dim_opportunity_status")


# ── Task 2: ejecutar SP ──────────────────────────────────────
@task(name="ejecutar-sp-lifecycle", retries=2, retry_delay_seconds=20)
def ejecutar_sp_lifecycle(fecha_desde: str, fecha_hasta: str, usuario: str = "prefect"):
    logger = get_run_logger()
    engine = mysql_connection.engine

    sql = text("CALL sp_build_opportunity_lifecycle(:fd, :fh, :u)")

    with engine.begin() as conn:
        result = conn.execute(sql, {
            "fd": fecha_desde,
            "fh": fecha_hasta,
            "u":  usuario
        })
        try:
            rows = result.fetchall()
            logger.info(f"Resultado SP: {rows}")
        except Exception:
            logger.info("SP ejecutado — sin resultset legible.")


# ── Task 3: validar log de resultado ────────────────────────
@task(name="validar-resultado-lifecycle", retries=1, retry_delay_seconds=10)
def validar_resultado_lifecycle():
    logger = get_run_logger()
    engine = mysql_connection.engine

    query = """
        SELECT estado, registros_afectados, mensaje
        FROM etl_execution_log_lifecycle
        ORDER BY id DESC LIMIT 1
    """
    with engine.connect() as conn:
        row = conn.execute(text(query)).mappings().first()

    if not row:
        raise ValueError("Sin registro en etl_execution_log_lifecycle")

    logger.info(f"Ultimo log: {dict(row)}")

    if row["estado"] != "SUCCESS":
        raise ValueError(f"SP termino en error: {row['mensaje']}")