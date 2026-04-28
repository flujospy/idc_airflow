from prefect import flow, get_run_logger
from datetime import datetime, timedelta
from typing import Optional

from tasks.crm_lifecycle import (
    validar_estados_no_mapeados,
    ejecutar_sp_lifecycle,
    validar_resultado_lifecycle,
)



# ── Hook de falla ────────────
def on_failure_hook(flow, flow_run, state):
    try:
        from tasks.notifications import send_notification  
        send_notification.fn(
            mensaje=(
                f":red_circle: *{flow.name}* falló\n"
                f"*Run ID:* `{flow_run.id}`\n"
                f"*Error:* {state.message or 'sin detalle'}"
            ),
            metadata={
                "flujo": flow.name,
                "run_id": str(flow_run.id),
                "estado": state.name
            }
        )
    except Exception:
        pass 


# ── Flujo principal ──────────────────────────────────────────
@flow(
    name="crm-opportunity-lifecycle",
    log_prints=True,
    on_failure=[on_failure_hook]
)
def flujo_opportunity_lifecycle(
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
    ventana_dias: int = 1,
    usuario: str = "prefect"
):
    logger = get_run_logger()

    # Calcular ventana si no viene explicita
    if not fecha_hasta:
        fecha_hasta = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    if not fecha_desde:
        fecha_desde = (
            datetime.utcnow() - timedelta(days=ventana_dias)
        ).strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Ventana: {fecha_desde}  →  {fecha_hasta}")
    logger.info(f"Usuario ejecución: {usuario}")

    # ── Orden de ejecucion ───────────────────────────────────
    validar_estados_no_mapeados()                         
    ejecutar_sp_lifecycle(                                
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        usuario=usuario
    )
    validar_resultado_lifecycle()                          