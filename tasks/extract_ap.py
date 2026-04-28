from prefect import task, get_run_logger
import pandas as pd
import requests
from typing import Dict, Any
from config.settings import settings


@task(
    name="Extracción de movimientos (aportes y retiros)",
    retries=1,
    retry_delay_seconds=60,
    timeout_seconds=300,
)
def extract_movimientos_batch(
    fecha_inicio: str,
    fecha_final: str,
    headers: Dict[str, str],
    endpoint: str = "/General/ConsultarMovimientos",
    codigo_persona: str = "M",
    codigo_cuenta: str = "M",
    codigo_producto: str = "M",
    tipo_movimiento: str = "M",
) -> pd.DataFrame:
    """
    Extrae movimientos (aportes / retiros) de la API en un rango de fechas.

    Payload esperado por la API:
    {
        "filtros": {
            "fechaInicio": "...",
            "fechaFinal": "...",
            "codigoPersona": "M",
            "codigoCuenta": "M",
            "codigoProducto": "M",
            "tipoMovimiento": "M"
        }
    }
    """
    logger = get_run_logger()
    logger.info(f"[MOV_EXTRACT] Extrayendo movimientos {fecha_inicio}..{fecha_final}")

    # 1) URL base (primero API_BASE_URL_AR, si no, API_BASE_URL normal)
    base_url = (
        getattr(settings, "API_BASE_URL_AR", None)
        or getattr(settings, "API_BASE_URL", "")
    )
    base_url = str(base_url).rstrip("/")

    if not base_url:
        raise RuntimeError("Falta configurar API_BASE_URL_AR o API_BASE_URL en settings.")

    # 2) Armar endpoint completo
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint

    url = f"{base_url}{endpoint}"

    # 3) Logging seguro de headers (sin mostrar token real)
    headers_preview = {
        k: (v if k.lower() != "authorization" else "Bearer ***")
        for k, v in headers.items()
    }
    logger.info(f"[MOV_EXTRACT] URL: {url!r}")
    logger.info(f"[MOV_EXTRACT] Headers: {headers_preview}")

    # 4) Payload según tu especificación
    payload: Dict[str, Any] = {
        "filtros": {
            "fechaInicio": fecha_inicio,
            "fechaFinal": fecha_final,
            "codigoPersona": codigo_persona,
            "codigoCuenta": codigo_cuenta,
            "codigoProducto": codigo_producto,
            "tipoMovimiento": tipo_movimiento,
        }
    }

    logger.info(f"[MOV_EXTRACT] Payload: {payload}")

    # 5) Llamada HTTP POST
    resp = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=getattr(settings, "API_TIMEOUT", 60),
    )

    # 6) Manejo de códigos de respuesta
    if resp.status_code == 404:
        logger.warning(f"[MOV_EXTRACT] 404 para {fecha_inicio}..{fecha_final} → sin datos")
        return pd.DataFrame()

    resp.raise_for_status()
    data = resp.json()

    # 7) Convertir respuesta a DataFrame
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        # Algunas APIs devuelven los datos bajo "data" o una lista en alguna clave
        records = data.get("data")
        if not isinstance(records, list):
            records = next(
                (v for v in data.values() if isinstance(v, list)),
                [],
            )
    else:
        records = []

    df = pd.DataFrame(records)
    logger.info(f"[MOV_EXTRACT] Total extraído: {len(df)} registros {fecha_inicio}..{fecha_final}")
    return df
