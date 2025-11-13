from sqlalchemy import create_engine
from config.settings import settings

_engine = None

def get_engine():
    """
    Devuelve una conexión (engine) SQLAlchemy reutilizable.
    Lee las credenciales desde config/settings.py
    """
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.mysql_connection_string,      
            pool_pre_ping=True,
            pool_recycle=3600
        )
    return _engine