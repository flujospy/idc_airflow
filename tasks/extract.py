from prefect import task, get_run_logger
from datetime import timedelta
import pandas as pd
import requests
from typing import Optional, Any, Dict
from config.settings import settings


# =========================
# AUTHENTICACIÓN
# =========================
@task(
    name="Autenticar API",
    retries=0,                 
    retry_delay_seconds=60,
    timeout_seconds=60
)
def authenticate_api() -> Dict[str, str]:
    """
    Obtiene un token (o usa el existente en settings.API_TOKEN) y devuelve headers.
    Estructura de retorno: {"Authorization": "Bearer ...", "Content-Type": "application/json"}
    """
    logger = get_run_logger()

    
    if getattr(settings, "API_TOKEN", None):
        token = str(settings.API_TOKEN).strip()
        if token:
            logger.info("Usando API_TOKEN del settings (.env).")
            return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # 2) Si no hay token directo, intenta login con URL + credenciales
    auth_url = getattr(settings, "API_AUTH_URL", None)
    user     = getattr(settings, "API_USERNAME", None)
    pwd      = getattr(settings, "API_PASSWORD", None)

    if not (auth_url and user and pwd):
        raise RuntimeError("No hay API_TOKEN ni credenciales/API_AUTH_URL para obtener token.")

    logger.info("Obteniendo token de autenticación...")
    
    payload_candidates = [
        {"username": user, "password": pwd},
        {"user": user, "password": pwd},
        {"UserName": user, "Password": pwd},
        {"User": user, "Password": pwd},
    ]

    last_err = None
    for body in payload_candidates:
        try:
            resp = requests.post(auth_url, json=body, timeout=30)
            # Log de diagnóstico (sin secretos)
            try:
                body_preview = resp.text[:500]
            except Exception:
                body_preview = "<no body>"
            logger.info(f"[AUTH] status={resp.status_code} body={body_preview}")

            if resp.status_code == 200:
                data = resp.json()
                # Intenta varias llaves típicas
                token = (
                    (isinstance(data, dict) and (
                        data.get("access_token")
                        or data.get("token")
                        or data.get("sessionKey")
                        or data.get("SessionKey")
                        or (isinstance(data.get("data"), dict) and data["data"].get("token"))
                    ))
                    or None
                )

                # Búsqueda superficial si viene anidado
                if not token and isinstance(data, dict):
                    for v in data.values():
                        if isinstance(v, dict):
                            token = v.get("token") or v.get("access_token") or v.get("SessionKey") or v.get("sessionKey")
                            if token:
                                break

                if token and str(token).strip() and str(token).strip().lower() != "none":
                    token = str(token).strip()
                    logger.info("Token obtenido exitosamente.")
                    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

                raise ValueError(f"Login OK, pero no se encontró token usable en la respuesta: {data}")

            elif resp.status_code in (400, 401, 403):
                last_err = f"Login rechazado ({resp.status_code}): {resp.text[:200]}"
            else:
                last_err = f"Error HTTP {resp.status_code}: {resp.text[:200]}"

        except requests.RequestException as e:
            last_err = f"RequestException: {repr(e)}"

    raise RuntimeError(f"No se pudo obtener token desde {auth_url}. Último error: {last_err}")


# =========================
# EXTRACCIÓN POR LOTE
# =========================
@task(
    name="Extraer datos de API por lote",
    retries=1,
    retry_delay_seconds=60,
    timeout_seconds=300
)
def extract_from_api_batch(
    fecha_inicio: str,
    fecha_fin: str,
    headers: Dict[str, str],                    
    endpoint: str = "/General/ConsultarSaldos",
    numero_identificacion: Optional[str] = None,
    codigo_producto: Optional[int] = None,
    tipo_producto: str = "M",
    tipo_cliente: str = "M",
) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Extrayendo datos del {fecha_inicio} al {fecha_fin}")

    base_url = str(getattr(settings, "API_BASE_URL", "")).rstrip("/")
    if not base_url:
        raise RuntimeError("Falta settings.API_BASE_URL")
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    url = f"{base_url}{endpoint}"

    
    headers_preview = {k: (v if k.lower() != "authorization" else "Bearer ***") for k, v in headers.items()}
    logger.info(f"URL: {url!r}")
    logger.info(f"Headers: {headers_preview}")

    payload: Dict[str, Any] = {
        "tipoConsulta": {"producto": tipo_producto, "cliente": tipo_cliente},
        "filtros": {
            "fechaInicio": fecha_inicio,
            "fechaCorte":  fecha_fin,
        }
    }
    if codigo_producto is not None:
        payload["filtros"]["codigoProducto"] = codigo_producto
    if numero_identificacion:
        payload["filtros"]["numeroIdentificacion"] = numero_identificacion

    logger.info(f"Payload: {payload}")

    try:
        resp = requests.post(
            url,
            headers=headers,                     
            json=payload,
            timeout=getattr(settings, "API_TIMEOUT", 60)
        )

        if resp.status_code == 404:
            logger.warning(f"404 para {fecha_inicio}..{fecha_fin} → sin datos")
            return pd.DataFrame()

        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, dict):
            if isinstance(data.get("data"), list):
                records = data["data"]
            elif isinstance(data.get("results"), list):
                records = data["results"]
            else:
                possible = [v for v in data.values() if isinstance(v, list)]
                records = possible[0] if possible else []
        elif isinstance(data, list):
            records = data
        else:
            records = []

        df = pd.DataFrame(records)
        logger.info(f"Total extraído: {len(df)} registros del {fecha_inicio} al {fecha_fin}")
        return df

    except requests.RequestException as e:
        logger.error(f"Error HTTP en extracción: {str(e)}")
        raise
