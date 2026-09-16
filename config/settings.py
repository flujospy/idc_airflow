# config/settings.py
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, ClassVar
from pathlib import Path
import json

class Settings(BaseSettings):
    """Configuración central de la app (via .env y archivos locales)."""

   
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # =========================
    # API (credenciales)
    # =========================

    API_USERNAME: Optional[str] = None
    API_PASSWORD: Optional[str] = None

    # ======================================================
    # API SALDOS 
    # ======================================================

    API_BASE_URL: str                      
    API_AUTH_URL: Optional[str] = None     

    # ======================================================
    # API APORTES Y RETIROS (NUEVA API)
    # ======================================================
    API_BASE_URL_AR: Optional[str] = None      # 
    API_AUTH_URL_AR: Optional[str] = None      #
    
    API_TOKEN: Optional[str] = None       
    BATCH_DAYS: int = 5

    # Encabezados / extracción del token
    API_TOKEN_HEADER: str = "Authorization"          
    API_TOKEN_PREFIX: str = "Bearer "                
    API_TOKEN_JSON_KEY: str = "token,access_token,jwt"
    API_TIMEOUT: int = 30
    API_TOKEN_TTL_SECONDS: int = 240                 

    # =========================
    # MySQL
    # =========================
    MYSQL_CONN_STR: Optional[str] = None             
    mysql_connection_string: Optional[str] = None    

    # =========================
    # PostgreSQL
    # =========================
    POSTGRES_CONN_STR: Optional[str] = None
    postgres_connection_string: Optional[str] = None

    # =========================
    # Prefect 
    # =========================
    PREFECT_API_URL: Optional[str] = None

    # =========================
    # Control de ETL / Auditoría
    # =========================
    ETL_CONTROL_TABLE: str = "etl_control"  
    ETL_RUN_LOGS_TABLE: str = "etl_run_logs"       
    ETL_LOGS_TABLE: str = "etl_run_logs"
    ETL_CONTROL_SCHEMA: Optional[str] = None     
    # =========================
    # Excel_files 
    # =========================

    ONEDRIVE_EXCEL_PATH_TABLA_MAESTRA: Optional[str] = None


    #ONEDRIVE_EXCEL_TABLA_MAESTRA_URL="C:\Users\user1\OneDrive - GRUPO IDC\DATOS\Tablas Datos.xlsx"
    # =========================
    # Notificaciones 
    # =========================
    SLACK_WEBHOOK: Optional[str] = None              
    SLACK_MENTIONS: Optional[str] = None             

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
            
            self.PATH_INPUT = self.PATH_INPUT or None
            self.PATH_OUTPUT = self.PATH_OUTPUT or None
            self.PATH_LOGS = self.PATH_LOGS or None


# ===== Instancia global de settings =====
settings = Settings()


if not settings.mysql_connection_string and settings.MYSQL_CONN_STR:
    settings.mysql_connection_string = settings.MYSQL_CONN_STR


if not settings.postgres_connection_string and settings.POSTGRES_CONN_STR:
    settings.postgres_connection_string = settings.POSTGRES_CONN_STR



# Carga rutas del JSON si existe 
settings.load_file_paths()