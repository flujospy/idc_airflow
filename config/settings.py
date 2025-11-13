# config/settings.py
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, ClassVar
from pathlib import Path
import json

class Settings(BaseSettings):
    """Configuración central de la app (via .env y archivos locales)."""

    # --- Pydantic v2: configuración de carga de entorno ---
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # =========================
    # API (Snap Compliance)
    # =========================
    API_BASE_URL: str                      # p.ej. https://idc-external-api.snap-compliance.com
    API_AUTH_URL: Optional[str] = None     # p.ej. https://idc-auth-api.snap-compliance.com/Login
    API_USERNAME: Optional[str] = None
    API_PASSWORD: Optional[str] = None
    API_TOKEN: Optional[str] = None        # opcional: token fijo para pruebas

    BATCH_DAYS: int = 15

    # Encabezados / extracción del token
    API_TOKEN_HEADER: str = "Authorization"          # o "X-Session-Key"
    API_TOKEN_PREFIX: str = "Bearer "                # incluye el espacio si aplica
    API_TOKEN_JSON_KEY: str = "token,access_token,jwt"
    API_TIMEOUT: int = 30
    API_TOKEN_TTL_SECONDS: int = 240                 # fallback si el JWT no trae exp

    # =========================
    # MySQL
    # =========================
    MYSQL_CONN_STR: Optional[str] = None             # nombre alterno
    mysql_connection_string: Optional[str] = None    # nombre “oficial” usado por connections.py

    # =========================
    # Prefect (opcional)
    # =========================
    PREFECT_API_URL: Optional[str] = None

    # =========================
    # Control de ETL / Auditoría
    # =========================
    ETL_CONTROL_TABLE: str = "etl_control"   # default, pero lo sobreescribiremos por .env
    ETL_RUN_LOGS_TABLE: str = "etl_run_logs"      # default
    ETL_LOGS_TABLE: str = "etl_run_logs"
    ETL_CONTROL_SCHEMA: Optional[str] = None      # p.ej. "public", "dbo"; si None no se usa schema
    

    # =========================
    # Notificaciones (Slack)
    # =========================
    SLACK_WEBHOOK: Optional[str] = None              # si None, se omite notificación
    SLACK_MENTIONS: Optional[str] = None             # p.ej. "<@UXXXX> @canal" (opcional)

    # =========================
    # Config ETL por archivo JSON (rutas locales)
    # =========================
    etl_config_path: ClassVar[Path] = Path(__file__).parent / "etl_config.json"
    PATH_INPUT: Optional[Path] = None
    PATH_OUTPUT: Optional[Path] = None
    PATH_LOGS: Optional[Path] = None

    def load_file_paths(self) -> None:
        """Carga rutas locales desde config/etl_config.json si existe (opcional)."""
        try:
            if self.etl_config_path.exists():
                with open(self.etl_config_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                paths = data.get("paths", {})
                self.PATH_INPUT = (
                    Path(paths["input_dir"]).resolve() if paths.get("input_dir") else None
                )
                self.PATH_OUTPUT = (
                    Path(paths["output_dir"]).resolve() if paths.get("output_dir") else None
                )
                self.PATH_LOGS = (
                    Path(paths["logs_dir"]).resolve() if paths.get("logs_dir") else None
                )
        except Exception:
            # No rompemos settings por un JSON de rutas malformado
            self.PATH_INPUT = self.PATH_INPUT or None
            self.PATH_OUTPUT = self.PATH_OUTPUT or None
            self.PATH_LOGS = self.PATH_LOGS or None


# ===== Instancia global de settings =====
settings = Settings()

# Unifica cadena de conexión MySQL si usas la variable antigua
if not settings.mysql_connection_string and settings.MYSQL_CONN_STR:
    settings.mysql_connection_string = settings.MYSQL_CONN_STR



# Carga rutas del JSON si existe (opcional, no crítico)
settings.load_file_paths()