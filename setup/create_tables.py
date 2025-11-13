from sqlalchemy import text
from config.connections import mysql_connection
from config.settings import settings

def create_control_tables():
    """Crea las tablas de control si no existen"""
    
    # Tabla de control principal
    control_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {settings.ETL_CONTROL_TABLE} (
        entity_name VARCHAR(100) PRIMARY KEY,
        last_run DATETIME NOT NULL,
        last_since VARCHAR(100),
        last_status VARCHAR(20) NOT NULL,
        last_rows_processed INT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_last_run (last_run),
        INDEX idx_last_status (last_status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    
    # Tabla de logs de ejecución
    logs_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {settings.ETL_LOGS_TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(100) NOT NULL,
        entity_name VARCHAR(100) NOT NULL,
        start_time DATETIME NOT NULL,
        end_time DATETIME,
        status VARCHAR(20) NOT NULL,
        rows_processed INT DEFAULT 0,
        error_msg TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_run_id (run_id),
        INDEX idx_entity_name (entity_name),
        INDEX idx_start_time (start_time),
        INDEX idx_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    
    with mysql_connection.engine.connect() as conn:
        print("Creando tabla de control...")
        conn.execute(text(control_table_sql))
        print(f" Tabla {settings.ETL_CONTROL_TABLE} creada")
        
        print("Creando tabla de logs...")
        conn.execute(text(logs_table_sql))
        print(f" Tabla {settings.ETL_LOGS_TABLE} creada")
        
        conn.commit()
    
    print("\n Tablas de control creadas exitosamente")


if __name__ == "__main__":
    create_control_tables()