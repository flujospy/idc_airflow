from prefect import flow, task,get_run_logger
from sqlalchemy import create_engine, text
from datetime import date
import calendar
import time

from config.settings import settings

@task
def obtener_parametros_mes(anio: int | None = None, mes: int | None = None) -> dict:
    hoy = date.today()

    anio = anio or hoy.year
    mes = mes or hoy.month

    ultimo_dia = calendar.monthrange(anio,mes)[1]

    return  {
        "anio":anio,
        "mes":mes,
        "periodo_inicio":f"{anio}-01",
        "periodo_fin":f"{anio}-{mes:02d}",
        "fecha_inicio":f"{anio}-01-01",
        "fecha_fin":f"{anio}-{mes:02d}-{ultimo_dia:02d}",
    }


@task
def construir_procedimientos(params: dict) -> list[dict]:
    anio = params["anio"]
    mes = params["mes"]

    return [
        {
            "nombre": "Actualizar saldos promedio mensual histórico",
            "sql": f"CALL sp_actualizar_saldos_promedio_mensual_hist({anio},{mes});"
        },
        {
            "nombre": "Cargar comisiones líquidos",
            "sql": f"CALL sp_load_comisiones_liquidos('{params['periodo_inicio']}','{params['periodo_fin']}');"
        },
        {
            "nombre": "Actualizar promedio mensual metas",
            "sql": f"CALL ps_actualizar_promedio_mensual_metas({anio},{mes},'ADMIN');"
        },
        {
            "nombre": "Cargar comisiones FPFQ",
            "sql": f"CALL sp_load_comisiones_fpfq('{params['fecha_inicio']}','{params['fecha_fin']}');"
        },
        {
            "nombre": "Cargar comisiones CAFIF",
            "sql": f"CALL sp_load_comisiones_cafif('{params['periodo_inicio']}','{params['periodo_fin']}');"
        },
    ]

@task(retries=0)
def ejecutar_sp(nombre: str, sql: str) -> dict:
    logger = get_run_logger()

    logger.info("=" * 70)
    logger.info(f"Iniciando SP: {nombre}")
    logger.info(f"SQL: {sql}")

    inicio = time.time()

    try:
        engine = create_engine(settings.mysql_connection_string)

        with engine.begin() as conn:
            result = conn.execute(text(sql))

            try:
                filas=result.fetchall()
            except:
                filas = []



        duracion = round(time.time() - inicio, 2)

        logger.info(f"Sp completado : {nombre}")
        logger.info(f"Duracion: {duracion} segundos")
        logger.info(f"Resultado: {filas}")

        return {
            "nombre":nombre,
            "status":"SUCCESS",
            "duracion_segundos":duracion,
            "error": None,
        }
    except Exception as e:
        duracion = round(time.time() - inicio,2)

        logger.error(f"Error ejecutnado SP: {nombre}")
        logger.error(str(e))


        return {
            "nombre":nombre,
            "status":"FAILED",
            "duracion_segundos": duracion,
            "error": str(e)
        }
    
@flow(name="SP Mensuales Fondos")
def flow_sp_mensual(anio: int | None = None, mes: int | None = None):
    logger = get_run_logger()

    logger.info("Iniciando flow mensual de stored procedures")

    params = obtener_parametros_mes(anio, mes)
    procedimientos = construir_procedimientos(params)

    logger.info(f"Periodo de ejecución: {params['anio']}-{params['mes']:02d}")

    resultados = []

    for sp in procedimientos:
        resultado = ejecutar_sp(
            nombre=sp["nombre"],
            sql=sp["sql"]
        )

        resultados.append(resultado)

        if resultado["status"] == "FAILED":
            logger.error("Se detiene el flow porque falló un SP.")
            raise Exception(f"Falló el SP: {resultado['nombre']}")

    logger.info("Todos los SP finalizaron correctamente")

    return {
        "status": "SUCCESS",
        "periodo": f"{params['anio']}-{params['mes']:02d}",
        "resultados": resultados,
    }


if __name__ == "__main__":
    flow_sp_mensual()
