-- ============================================================
-- Proyecto: Migración MySQL db_fondos_midas -> PostgreSQL
-- Archivo : 01_create_raw_tables.sql
-- Objetivo: Crear tablas base en schema raw
-- Motor   : PostgreSQL
-- ============================================================


-- ============================================================
-- Tabla: raw.cliente_maestro
-- Descripción:
-- Catálogo maestro de clientes usado para enriquecer saldos,
-- movimientos, comisiones, cartera y gerente.
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.cliente_maestro (
    cod_persona TEXT NULL,
    nombre TEXT NULL,
    distribucion TEXT NULL,
    gerente TEXT NULL,
    cartera TEXT NULL,
    tipo_cliente TEXT NULL,
    tipo_movimiento TEXT NULL,
    empresa_de_grupo_idc TEXT NULL
);

COMMENT ON TABLE raw.cliente_maestro IS
'Tabla raw migrada desde MySQL db_fondos_midas.cliente_maestro. Contiene clasificación comercial y asignación de clientes.';


-- ============================================================
-- Tabla: raw.movimiento
-- Descripción:
-- Movimientos transaccionales. Tabla crítica para cálculos CAFIF
-- y comisiones.
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.movimiento (
    id_movimiento VARCHAR(255) NOT NULL,
    fecha_movimiento TIMESTAMP NOT NULL,
    fecha DATE NULL,
    numero_movimiento BIGINT NULL,
    codigo_persona VARCHAR(50) NULL,
    nombre_cliente VARCHAR(255) NULL,
    monto NUMERIC(18,2) NULL,
    saldo_anterior NUMERIC(18,2) NULL,
    saldo_resultante NUMERIC(18,2) NULL,
    participaciones NUMERIC(18,6) NULL,
    participaciones_restantes NUMERIC(18,6) NULL,
    valor_participacion NUMERIC(18,6) NULL,
    saldo_participaciones_anterior NUMERIC(18,6) NULL,
    codigo_cuenta BIGINT NULL,
    numero_cuenta BIGINT NULL,
    producto VARCHAR(100) NULL,
    tipo_movimiento VARCHAR(50) NULL,
    tipo_observaciones TEXT NULL,
    hecho_por VARCHAR(100) NULL,
    codigo_movimiento VARCHAR(100) NULL,
    fecha_registro TIMESTAMP NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    nombrecuenta VARCHAR(255) NULL,

    es_cafif SMALLINT GENERATED ALWAYS AS (
        CASE
            WHEN producto ILIKE '%(CAFIF)%' THEN 1
            ELSE 0
        END
    ) STORED,

    CONSTRAINT pk_movimiento PRIMARY KEY (id_movimiento)
);

COMMENT ON TABLE raw.movimiento IS
'Tabla raw migrada desde MySQL db_fondos_midas.movimiento. Insumo crítico para cálculo de comisiones y lógica CAFIF.';

COMMENT ON COLUMN raw.movimiento.es_cafif IS
'Columna generada en PostgreSQL para identificar movimientos asociados a CAFIF a partir del campo producto.';


-- ============================================================
-- Tabla: raw.tabla_saldos
-- Descripción:
-- Tabla histórica principal de saldos diarios/mensuales.
-- Se migra por rangos de fecha debido a su volumen.
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.tabla_saldos (
    numerocuenta INTEGER NULL,
    nombrecliente VARCHAR(255) NULL,
    nombrecuenta VARCHAR(255) NULL,
    codigopersona VARCHAR(30) NULL,
    numeroidentificacion VARCHAR(80) NULL,
    saldo NUMERIC(20,2) NULL,
    codigoejecutivo INTEGER NULL,
    nombreejecutivo VARCHAR(300) NULL,
    codigoproducto INTEGER NULL,
    nombreproducto VARCHAR(150) NULL,
    fecha TIMESTAMP NULL,
    tipopersona VARCHAR(15) NULL,
    valorparticipacion NUMERIC(10,2) NULL,
    participaciones NUMERIC(10,2) NULL,
    codigoreferenciabitacora VARCHAR(255) NULL,
    datos TEXT NULL,
    fechaarchivo TEXT NULL,
    capitalpagado NUMERIC(10,2) NULL,
    rendimiento NUMERIC(10,6) NULL,
    fecha_carga TIMESTAMP NULL,
    fuente TEXT NULL
);

COMMENT ON TABLE raw.tabla_saldos IS
'Tabla raw migrada desde MySQL db_fondos_midas.tabla_saldos. Contiene histórico principal de saldos.';

COMMENT ON COLUMN raw.tabla_saldos.codigopersona IS
'Código de persona del cliente. Equivale a codigoPersona en MySQL.';

COMMENT ON COLUMN raw.tabla_saldos.numerocuenta IS
'Número de cuenta. Equivale a numeroCuenta en MySQL.';

COMMENT ON COLUMN raw.tabla_saldos.codigoproducto IS
'Código de producto. Equivale a codigoProducto en MySQL.';