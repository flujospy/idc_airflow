"""
# ETL API a MySQL con Sistema de Control y Logs

ETL robusto en Prefect que extrae datos de una API, los procesa y los carga en MySQL,
con sistema completo de control de ejecuciones y logs detallados.

##  Características

-  Procesamiento en lotes para grandes volúmenes
-  Sistema de control de ejecuciones (etl_control)
-  Logs detallados por ejecución (etl_run_logs)
-  Reintentos automáticos
-  Caché de autenticación
-  Notificaciones a Slack
-  Manejo robusto de errores
-  Ejecuciones parciales (continúa si algunos lotes fallan)

##  Estructura del Proyecto
```
proyecto_etl/
├── config/
│   ├── __init__.py
│   ├── settings.py          # Configuración con Pydantic
│   └── connections.py       # Gestión de conexiones
├── tasks/
│   ├── __init__.py
│   ├── extract.py          # Tareas de extracción
│   ├── transform.py        # Tareas de transformación
│   ├── load.py             # Tareas de carga
│   ├── notifications.py    # Notificaciones
│   └── etl_control.py      # Control y logs
├── flows/
│   ├── __init__.py
│   └── etl_api_lotes.py    # Flow principal
├── utils/
│   ├── __init__.py
│   └── date_utils.py       # Utilidades de fechas
├── setup/
│   ├── __init__.py
│   └── create_tables.py    # Crear tablas de control
├── main.py                  # Punto de entrada
├── .env                     # Variables de entorno
├── .env.example
├── requirements.txt
└── README.md
```

##  Instalación

1. Clonar repositorio e instalar dependencias:
```bash
pip install -r requirements.txt
```

2. Configurar variables de entorno:
```bash
cp .env.example .env
# Editar .env con tus credenciales
```

3. Crear tablas de control:
```bash
python setup/create_tables.py
```

##  Tablas de Control

### etl_control
Mantiene el estado de cada entidad:
- `entity_name`: Identificador único de la entidad
- `last_run`: Fecha/hora de última ejecución
- `last_since`: Rango de fechas procesado
- `last_status`: Estado (SUCCESS, FAILED, PARTIAL)
- `last_rows_processed`: Registros procesados

### etl_run_logs
Log detallado de cada ejecución:
- `run_id`: ID único de ejecución
- `entity_name`: Entidad procesada
- `start_time` / `end_time`: Tiempos de ejecución
- `status`: Estado de la ejecución
- `rows_processed`: Registros procesados
- `error_msg`: Mensaje de error (si aplica)

##  Uso

### Ejecución básica:
```bash
python main.py
```

### Con parámetros personalizados:
```bash
python main.py \
  --fecha-inicio "2024-01-01" \
  --fecha-fin "2024-12-31" \
  --entity-name "ventas" \
  --tabla-destino "ventas_data" \
  --endpoint "/api/v1/ventas"
```

### Desde código Python:
```python
from flows.etl_api_lotes import etl_api_to_mysql_flow

result = etl_api_to_mysql_flow(
    fecha_inicio="2024-01-01",
    fecha_fin="2024-12-31",
    entity_name="ventas",
    tabla_destino="ventas_data",
    endpoint="/api/v1/ventas"
)
```

##  Monitoreo

### Ver últimas ejecuciones:
```sql
SELECT * FROM etl_control ORDER BY last_run DESC;
```

### Ver logs de una entidad:
```sql
SELECT * FROM etl_run_logs 
WHERE entity_name = 'ventas' 
ORDER BY start_time DESC 
LIMIT 10;
```

### Ver ejecuciones fallidas:
```sql
SELECT * FROM etl_run_logs 
WHERE status IN ('FAILED', 'PARTIAL')
ORDER BY start_time DESC;
```

##  Configuración

### Ajustar tamaño de lotes:
```env
BATCH_DAYS=15  # Procesar en lotes de 15 días
```

### Configurar reintentos:
En cada task, ajustar:
```python
@task(retries=3, retry_delay_seconds=60)
```

### Notificaciones Slack:
```env
SLACK_WEBHOOK=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

##  Personalización

### Agregar nueva transformación:
```python
# En tasks/transform.py
@task(name="Mi transformación")
def mi_transformacion(df: pd.DataFrame) -> pd.DataFrame:
    # Tu lógica aquí
    return df
```

### Cambiar estrategia de carga:
```python
# En tasks/load.py, modificar if_exists
load_to_mysql(df, table_name, if_exists="replace")  # Reemplazar
load_to_mysql(df, table_name, if_exists="append")   # Agregar
```

##  Características Avanzadas

### Ejecuciones Parciales
Si algunos lotes fallan pero otros son exitosos, el ETL:
- Marca el status como `PARTIAL`
- Carga los datos exitosos
- Registra qué lotes fallaron
- Permite reintentar solo los lotes fallidos

### Caché de Autenticación
El token de API se cachea por 50 minutos para evitar autenticaciones innecesarias.

### Procesamiento Concurrente
Usa `ConcurrentTaskRunner` para procesar múltiples lotes en paralelo.

##  Troubleshooting

### Error de conexión MySQL:
```bash
# Verificar credenciales en .env
# Asegurar que MySQL esté corriendo
```

### Error de autenticación API:
```bash
# Verificar API_USERNAME y API_PASSWORD
# Verificar que API_AUTH_URL sea correcta
```

### Lotes muy lentos:
```bash
# Reducir BATCH_DAYS en .env
# Aumentar MAX_CONCURRENT_BATCHES
```

##  Licencia

MIT License
"""