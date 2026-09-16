from sqlalchemy import text
from config.connections import postgres_connection
from tasks.migrate_mysql_to_postgres import migrate_tabla_saldos_by_year


def main():
    pg_engine = postgres_connection.engine

    # Limpiar tabla antes de carga completa
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw.tabla_saldos;"))

    for anio in range(2017, 2027):
        migrate_tabla_saldos_by_year(
            anio=anio,
            chunksize=50000
        )


if __name__ == "__main__":
    main()