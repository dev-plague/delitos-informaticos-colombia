-- Zona de servicio: tablas listas para el tablero (sin cálculos en el tablero).
-- Se crean al inicializar el contenedor de PostgreSQL y también las crea
-- src/load_service.py (CREATE TABLE IF NOT EXISTS) en cada corrida.

CREATE SCHEMA IF NOT EXISTS service;
CREATE SCHEMA IF NOT EXISTS control;

-- Control de cargas incrementales e idempotencia (marca de agua)
CREATE TABLE IF NOT EXISTS control.etl_control (
    fuente          TEXT        NOT NULL,
    dataset         TEXT        NOT NULL,
    ultima_fecha    TIMESTAMP   NOT NULL,
    ultima_corrida  TIMESTAMP   NOT NULL,
    registros_raw   BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (fuente, dataset)
);

-- Serie anual de casos
CREATE TABLE IF NOT EXISTS service.ind_casos_anio (
    anio        INTEGER PRIMARY KEY,
    casos       BIGINT  NOT NULL,
    registros   BIGINT  NOT NULL,
    participacion NUMERIC(6,4) NOT NULL
);

-- Casos por año y conducta (artículo del Código Penal)
CREATE TABLE IF NOT EXISTS service.ind_conducta_anio (
    anio        INTEGER NOT NULL,
    codigo      TEXT    NOT NULL,
    conducta    TEXT    NOT NULL,
    casos       BIGINT  NOT NULL,
    PRIMARY KEY (anio, codigo)
);

-- Casos por año y departamento
CREATE TABLE IF NOT EXISTS service.ind_departamento_anio (
    anio         INTEGER NOT NULL,
    departamento TEXT    NOT NULL,
    casos        BIGINT  NOT NULL,
    ranking      INTEGER NOT NULL,
    PRIMARY KEY (anio, departamento)
);

-- Top municipios del periodo
CREATE TABLE IF NOT EXISTS service.ind_municipio (
    municipio    TEXT    NOT NULL,
    departamento TEXT    NOT NULL,
    casos        BIGINT  NOT NULL,
    ranking      INTEGER NOT NULL,
    PRIMARY KEY (departamento, municipio)
);

-- Serie mensual (tendencia)
CREATE TABLE IF NOT EXISTS service.ind_mensual (
    anio_mes  TEXT    PRIMARY KEY,
    casos     BIGINT  NOT NULL,
    registros BIGINT  NOT NULL
);

-- Tabla de hechos de la zona curada (muestra para auditoría del tablero)
CREATE TABLE IF NOT EXISTS service.fct_delitos (
    id_registro   TEXT PRIMARY KEY,
    fecha_hecho   DATE        NOT NULL,
    anio          INTEGER     NOT NULL,
    mes           INTEGER     NOT NULL,
    cod_depto     TEXT        NOT NULL,
    departamento  TEXT        NOT NULL,
    cod_muni      TEXT        NOT NULL,
    municipio     TEXT        NOT NULL,
    codigo        TEXT        NOT NULL,
    conducta      TEXT        NOT NULL,
    cantidad      INTEGER     NOT NULL
);

-- KPIs de una fila que alimentan las tarjetas del tablero
CREATE TABLE IF NOT EXISTS service.ind_kpis (
    id            INTEGER PRIMARY KEY,
    total_casos   BIGINT,
    total_registros BIGINT,
    conducta_top  TEXT,
    casos_conducta_top BIGINT,
    depto_top     TEXT,
    casos_depto_top BIGINT,
    anio_pico     INTEGER,
    casos_anio_pico BIGINT,
    fecha_corte   DATE,
    actualizado   TIMESTAMP
);

-- Salida del flujo Structured Streaming (capa rápida, Lambda)
CREATE TABLE IF NOT EXISTS service.stream_diario (
    fecha      DATE    NOT NULL,
    codigo     TEXT    NOT NULL,
    conducta   TEXT    NOT NULL,
    casos      BIGINT  NOT NULL,
    actualizado TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (fecha, codigo)
);
