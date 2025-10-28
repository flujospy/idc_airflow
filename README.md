Este repositorio contiene la configuración y los archivos base para instalar, ejecutar y probar **Apache Airflow** en **Windows**, adaptando el flujo de instalación típico de Linux a PowerShell.

---

## Requisitos previos

- **Windows 10 o 11**
- **Python 3.11.7**
- **Pip** actualizado (`python -m pip install --upgrade pip`)
- **Git** (`winget install --id Git.Git -e`)
- **Visual Studio Code**

---



---

## Configuración inicial

```powershell
# Crear entorno virtual
python -m venv venv
.\venv\Scripts\activate

# Definir variables de entorno
setx AIRFLOW_HOME "C:\airflow-tutorial\airflow_home"
$env:AIRFLOW_HOME = "C:\airflow-tutorial\airflow_home"

# Instalar Airflow
pip install "apache-airflow==2.9.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
