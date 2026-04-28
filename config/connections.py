from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import base64
import json
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict
from .settings import settings


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _decode_jwt_exp(token: str) -> Optional[datetime]:
    """
    Intenta leer el 'exp' de un JWT sin validar la firma.
    Si no es JWT o no trae 'exp', retorna None.
    """
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1] + "==="  # padding
        payload_json = base64.urlsafe_b64decode(payload_b64).decode("utf-8")
        payload = json.loads(payload_json)
        exp = payload.get("exp")
        if exp is None:
            return None
        # exp es unix epoch seconds
        return datetime.fromtimestamp(int(exp), tz=timezone.utc)
    except Exception:
        return None


class APIConnection:
    """Gestiona la autenticación y conexión con la API"""

    def __init__(self):
        # Normaliza URLs
        self.base_url = (settings.API_BASE_URL_AR or settings.API_BASE_URL or "").rstrip("/")
        self.auth_url = (settings.API_AUTH_URL_AR or settings.API_AUTH_URL or "").strip() or None

        # Credenciales de login
        self.username: Optional[str] = settings.API_USERNAME or None
        self.password: Optional[str] = settings.API_PASSWORD or None

        # Token administrado internamente (temporal)
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

        # Config de token estático / headers
        self.static_token: Optional[str] = (getattr(settings, "API_TOKEN", None) or "").strip() or None
        self.token_header_name: str = getattr(settings, "API_TOKEN_HEADER", "Authorization")
        self.token_prefix: str = getattr(settings, "API_TOKEN_PREFIX", "Bearer ")  # incluye espacio si aplica

        # Timeout y TTL por defecto
        self.timeout: int = int(getattr(settings, "API_TIMEOUT", 30))
        self.default_ttl: int = int(getattr(settings, "API_TOKEN_TTL_SECONDS", 240))  # 4 min

        # Clave donde viene el token en el JSON de login
        self.token_json_keys: tuple[str, ...] = (
            getattr(settings, "API_TOKEN_JSON_KEY", None) or "token,access_token,jwt"
        ).split(",")


class APIConnectionAR(APIConnection):
    """
    Variante de APIConnection para la API de Aportes y Retiros.
    Reutiliza usuario/contraseña, pero usa otras URLs base.
    """
    def __init__(self):
        super().__init__()

        
        self.base_url = (settings.API_BASE_URL_AR or settings.API_BASE_URL or "").rstrip("/")
        self.auth_url = (settings.API_AUTH_URL_AR or settings.API_AUTH_URL or "").strip() or None

    # --------- Lógica de autenticación ---------

    def _login_and_get_token(self) -> str:
        if not self.auth_url or not self.username or not self.password:
            raise RuntimeError("No hay API_TOKEN ni credenciales/API_AUTH_URL para obtener token.")

        payload = {"username": self.username, "password": self.password}

        try:
            resp = requests.post(self.auth_url, json=payload, timeout=self.timeout)
            # Log liviano de diagnóstico (evitar credenciales)
            preview = resp.text[:400]
            if resp.status_code >= 400:
                raise RuntimeError(f"Auth HTTP {resp.status_code}: {preview}")

            data = resp.json()

            token = None
            for key in self.token_json_keys:
                key = key.strip()
                if not key:
                    continue
                # Soporta claves anidadas tipo "data.token"
                cur = data
                for part in key.split("."):
                    if isinstance(cur, dict) and part in cur:
                        cur = cur[part]
                    else:
                        cur = None
                        break
                if isinstance(cur, str) and cur.strip():
                    token = cur.strip()
                    break

            if not token:
                raise RuntimeError(f"No se recibió token en la respuesta: {data}")

            # Intenta deducir expiración desde exp del JWT
            exp = _decode_jwt_exp(token)
            if exp is None:
                # fallback TTL
                exp = _utcnow() + timedelta(seconds=self.default_ttl)
            # margen de seguridad de 60s
            self._token_expiry = exp - timedelta(seconds=60)
            self._token = token
            return token

        except requests.RequestException as e:
            raise RuntimeError(f"Error al obtener token de autenticación: {e}") from e

    def get_token(self) -> str:
        """
        Retorna un token actual. Prioriza token estático si existe.
        Si no, renueva automáticamente cuando vence.
        """
        if self.static_token:
            
            return self.static_token

        
        if self._token and self._token_expiry and _utcnow() < self._token_expiry:
            return self._token

        
        return self._login_and_get_token()

    def get_headers(self) -> Dict[str, str]:
        """
        Retorna headers listos para llamadas a la API.
        - Si hay API_TOKEN estático: usa header configurable.
        - Si no, asegura tener/renovar Bearer dinámico.
        """
        headers: Dict[str, str] = {"Content-Type": "application/json"}

        if self.static_token:
            headers[self.token_header_name] = f"{self.token_prefix}{self.static_token}".strip()
            return headers

        token = self.get_token()  
        
        headers[self.token_header_name] = f"{self.token_prefix}{token}".strip()
        return headers

    
    def build_url(self, path: str) -> str:
        path = path or ""
        if not self.base_url:
            raise RuntimeError("API_BASE_URL no configurada.")
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return f"{self.base_url}/{path.lstrip('/')}"


class MySQLConnection:
    """Gestiona la conexión a MySQL con SQLAlchemy"""

    def __init__(self):
        self._engine = None
        self._session_factory = None

    @property
    def engine(self):
        if self._engine is None:
           
            conn_str = getattr(settings, "mysql_connection_string", None)
            if not conn_str:
                raise RuntimeError("mysql_connection_string no está definido en settings.")
            self._engine = create_engine(
                conn_str,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
            )
        return self._engine

    @contextmanager
    def get_session(self):
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)

        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


# Instancias globales
# API Saldos 
api_connection = APIConnection()

# API Aportes/Retiros 
api_connection_ar = APIConnectionAR()

# MySQL
mysql_connection = MySQLConnection()