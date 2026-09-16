import pandas as pd
from sqlalchemy import text
from config.connections import mysql_connection, postgres_connection


def migrate_table_full(
    source_table: str,
    target_schema: str,
    target_table: str,
    chunksize: int = 50000,
    truncate_target: bool = True
):
    """
    Migra una tabla completa desde MySQL hacia PostgreSQL.
    Pensado para tablas pequeñas y medianas.
    """

    mysql_engine = mysql_connection.engine
    pg_engine = postgres_connection.engine

    full_target = f"{target_schema}.{target_table}"

    if truncate_target:
        with pg_engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {full_target};"))

    query = f"SELECT * FROM {source_table}"

    total_rows = 0

    for chunk in pd.read_sql(query, mysql_engine, chunksize=chunksize):
        chunk.to_sql(
            name=target_table,
            con=pg_engine,
            schema=target_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )

        total_rows += len(chunk)
        print(f"{source_table} -> {full_target}: {total_rows} registros cargados")

    return total_rows

def migrate_movimiento(
    chunksize: int = 50000,
    truncate_target: bool = True
):
    """
    Migra db_fondos_midas.movimiento hacia raw.movimiento.
    Excluye es_cafif porque en PostgreSQL es columna generada.
    """

    mysql_engine = mysql_connection.engine
    pg_engine = postgres_connection.engine

    target_schema = "raw"
    target_table = "movimiento"
    full_target = f"{target_schema}.{target_table}"

    if truncate_target:
        with pg_engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {full_target};"))

    query = """
        SELECT
            id_movimiento,
            fecha_movimiento,
            fecha,
            numero_movimiento,
            codigo_persona,
            nombre_cliente,
            monto,
            saldo_anterior,
            saldo_resultante,
            participaciones,
            participaciones_restantes,
            valor_participacion,
            saldo_participaciones_anterior,
            codigo_cuenta,
            numero_cuenta,
            producto,
            tipo_movimiento,
            tipo_observaciones,
            hecho_por,
            codigo_movimiento,
            fecha_registro,
            created_at,
            updated_at,
            nombreCuenta AS nombrecuenta
        FROM db_fondos_midas.movimiento
    """

    total_rows = 0

    for chunk in pd.read_sql(query, mysql_engine, chunksize=chunksize):
        chunk.to_sql(
            name=target_table,
            con=pg_engine,
            schema=target_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )

        total_rows += len(chunk)
        print(f"db_fondos_midas.movimiento -> {full_target}: {total_rows} registros cargados")

    return total_rows


def migrate_tabla_saldos_by_year(
    anio: int,
    chunksize: int = 50000
):
    """
    Migra db_fondos_midas.tabla_saldos por año hacia raw.tabla_saldos.
    Usa alias en minúsculas para evitar problemas con PostgreSQL.
    """

    mysql_engine = mysql_connection.engine
    pg_engine = postgres_connection.engine

    target_schema = "raw"
    target_table = "tabla_saldos"
    full_target = f"{target_schema}.{target_table}"

    query = f"""
        SELECT
            numeroCuenta AS numerocuenta,
            nombreCliente AS nombrecliente,
            nombrecuenta AS nombrecuenta,
            codigoPersona AS codigopersona,
            numeroIdentificacion AS numeroidentificacion,
            saldo,
            codigoEjecutivo AS codigoejecutivo,
            nombreEjecutivo AS nombreejecutivo,
            codigoProducto AS codigoproducto,
            nombreProducto AS nombreproducto,
            fecha,
            tipoPersona AS tipopersona,
            valorParticipacion AS valorparticipacion,
            participaciones,
            codigoReferenciaBitacora AS codigoreferenciabitacora,
            datos,
            FechaArchivo AS fechaarchivo,
            capitalPagado AS capitalpagado,
            rendimiento,
            fecha_carga,
            fuente
        FROM db_fondos_midas.tabla_saldos
        WHERE fecha >= '{anio}-01-01'
          AND fecha <  '{anio + 1}-01-01'
    """

    total_rows = 0

    for chunk in pd.read_sql(query, mysql_engine, chunksize=chunksize):
        chunk.to_sql(
            name=target_table,
            con=pg_engine,
            schema=target_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )

        total_rows += len(chunk)
        print(f"tabla_saldos {anio} -> {full_target}: {total_rows} registros cargados")

    return total_rows