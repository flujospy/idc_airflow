-- ============================================================
-- Proyecto: Migración MySQL db_fondos_midas -> PostgreSQL
-- Archivo : 00_create_schemas.sql
-- Objetivo: Crear schemas base para arquitectura de datos
-- Motor   : PostgreSQL
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS audit;

COMMENT ON SCHEMA raw IS 'Capa de datos fuente migrados desde MySQL sin transformación fuerte.';
COMMENT ON SCHEMA staging IS 'Capa intermedia para preparación, limpieza y cálculos temporales.';
COMMENT ON SCHEMA curated IS 'Capa de datos procesados, consolidados y listos para consumo analítico.';
COMMENT ON SCHEMA audit IS 'Capa de control, logs, auditoría y trazabilidad de procesos ETL.';