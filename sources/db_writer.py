"""
sources/db_writer.py
Escritura de datos hacia MySQL usando SQLAlchemy + PyMySQL.
Maneja creacion automatica de tablas, TRUNCATE e INSERT por chunks.

En MySQL no hay schemas dentro de una base: el 'target_schema' del JSON
es directamente la base de datos destino (ej: "raw" -> `raw`.`tabla`).

Estrategia de tipos para strings (siempre utf8mb4):

  CON nvarchar_max_length en el JSON:
    -> Todas las columnas string usan ese tamanio fijo (VARCHAR(n))
    -> No se consulta schema origen ni se infiere desde el chunk
    -> Recomendado para tablas con datos inconsistentes con el schema

  SIN nvarchar_max_length:
    1. Schema del origen (HANA/Oracle/Postgres) -> largo exacto definido
    2. Inferencia desde el primer chunk -> redondea a 255/500/1000/2000/4000/LONGTEXT
    3. Columna completamente nula -> LONGTEXT

  Limite de fila de MySQL: la suma de los VARCHAR declarados no puede pasar
  65.535 bytes (utf8mb4 = 4 bytes por caracter). Tablas anchas como las de SAP
  lo superan facil, asi que si no entra se pasan a LONGTEXT las columnas mas
  anchas hasta que la fila quepa. Los TEXT no cuentan contra ese limite.
"""

import logging
import os
from datetime import date, datetime

from typing import Any, Iterator, Optional

from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger, Column, Date, MetaData, String, Table,
    create_engine, text,
)
from sqlalchemy.dialects.mysql import DATETIME, DOUBLE, LONGTEXT
from sqlalchemy.engine import URL, Engine

load_dotenv()

logger = logging.getLogger(__name__)

# Mayor VARCHAR que se declara. Por encima se usa LONGTEXT.
MAX_VARCHAR_LENGTH = 4000

# Limite de MySQL para la suma de columnas de una fila es 65.535 bytes.
# Se deja margen para los bytes de largo de cada VARCHAR y columnas fijas.
ROW_SIZE_BUDGET = 60000
BYTES_PER_CHAR  = 4   # utf8mb4

TABLE_KWARGS = {
    "mysql_engine":  "InnoDB",
    "mysql_charset": "utf8mb4",
    "mysql_collate": "utf8mb4_unicode_ci",
}


def get_destination_engine() -> Engine:
    """
    Crea y devuelve un engine SQLAlchemy para MySQL destino.

    URL.create escapa la password, asi que caracteres especiales
    (#, n, @, %) no rompen la conexion.

    Variables esperadas en .env:
        DESTINATION_HOST
        DESTINATION_PORT       (opcional, default 3306)
        DESTINATION_DATABASE   (base por defecto de la conexion, ej: raw)
        DESTINATION_USERNAME
        DESTINATION_PASSWORD   (usar comillas dobles si contiene #)
        DESTINATION_SSL_CA     (opcional, ruta al certificado CA de RDS)

    Compatibilidad: si no estan definidas las variables individuales,
    intenta usar DESTINATION_CONNECTION_STRING como antes.
    """
    host     = os.environ.get("DESTINATION_HOST")
    port     = int(os.environ.get("DESTINATION_PORT", "3306"))
    database = os.environ.get("DESTINATION_DATABASE")
    username = os.environ.get("DESTINATION_USERNAME")
    password = os.environ.get("DESTINATION_PASSWORD", "")
    ssl_ca   = os.environ.get("DESTINATION_SSL_CA")

    engine_kwargs = {
        # RDS corta conexiones inactivas: verificar antes de usar y reciclar
        "pool_pre_ping": True,
        "pool_recycle":  3600,
    }

    if host and database:
        url = URL.create(
            "mysql+pymysql",
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            query={"charset": "utf8mb4"},
        )
        connect_args = {"local_infile": False}
        if ssl_ca:
            connect_args["ssl"] = {"ca": ssl_ca}
        return create_engine(url, connect_args=connect_args, **engine_kwargs)

    # Fallback: connection string completa (legacy)
    conn_str = os.environ.get("DESTINATION_CONNECTION_STRING")
    if conn_str:
        return create_engine(conn_str, **engine_kwargs)

    raise EnvironmentError(
        "No se encontraron credenciales para el destino. "
        "Revisa .env: DESTINATION_HOST, DESTINATION_DATABASE, "
        "DESTINATION_USERNAME, DESTINATION_PASSWORD"
    )


def _q(identifier: str) -> str:
    """Cita un identificador MySQL con backticks (columnas SAP traen '/')."""
    return "`" + str(identifier).replace("`", "``") + "`"


def _qualified(schema: str, table_name: str) -> str:
    return f"{_q(schema)}.{_q(table_name)}"


def _string_type(length: Optional[int]):
    """
    Devuelve el tipo SQLAlchemy para una columna string:
      None o <= 0  -> LONGTEXT
      <= 4000      -> VARCHAR(length)
      > 4000       -> LONGTEXT
    """
    if not length or length <= 0 or length > MAX_VARCHAR_LENGTH:
        return LONGTEXT()
    return String(length)


def _string_type_sql(length: Optional[int]) -> str:
    """Igual que _string_type pero como texto para un ALTER TABLE."""
    if not length or length <= 0 or length > MAX_VARCHAR_LENGTH:
        return "LONGTEXT"
    return f"VARCHAR({length})"


def _fit_row_size(col_types: dict) -> dict:
    """
    Pasa a LONGTEXT las columnas VARCHAR mas anchas hasta que la fila
    entre en el limite de 65.535 bytes de MySQL.
    Sin esto CREATE TABLE falla con 'Row size too large' en tablas anchas.
    """
    varchars = {
        col: t.length for col, t in col_types.items()
        if isinstance(t, String) and not isinstance(t, LONGTEXT) and t.length
    }
    # Columnas fijas (BIGINT, DOUBLE, DATETIME) ocupan 8 bytes; TEXT unos 12
    fixed = 12 * (len(col_types) - len(varchars))
    total = fixed + sum(n * BYTES_PER_CHAR + 2 for n in varchars.values())

    if total <= ROW_SIZE_BUDGET:
        return col_types

    converted = 0
    for col, length in sorted(varchars.items(), key=lambda kv: kv[1], reverse=True):
        if total <= ROW_SIZE_BUDGET:
            break
        col_types[col] = LONGTEXT()
        total -= length * BYTES_PER_CHAR + 2 - 12
        converted += 1

    logger.info(
        "La fila superaba el limite de MySQL: %d columna(s) string pasaron a LONGTEXT.",
        converted,
    )
    return col_types


def _infer_column_types(
    chunk: list[dict],
    col_lengths: Optional[dict] = None,
    nvarchar_max_length: Optional[int] = None,
) -> dict:
    """
    Infiere los tipos de todas las columnas con esta prioridad para strings:

    1. col_lengths del schema origen -> largo exacto del sistema origen
    2. Inferencia desde el chunk    -> largo maximo medido en las filas
       capado por nvarchar_max_length si esta definido
    3. Columna completamente nula   -> nvarchar_max_length o LONGTEXT

    Args:
        chunk:               Primer chunk de datos.
        col_lengths:         {col_name: max_length} del schema origen.
        nvarchar_max_length: Tope maximo para strings inferidos desde el chunk.
                             None = sin tope (puede llegar a LONGTEXT).
    """
    if not chunk:
        return {}

    columns   = list(chunk[0].keys())
    col_types = {}

    for col in columns:
        values   = [row.get(col) for row in chunk]
        non_null = [v for v in values if v is not None]

        if not non_null:
            # Columna completamente nula - usar fallback
            col_types[col] = _string_type(nvarchar_max_length)
            continue

        sample = non_null[0]

        if isinstance(sample, bool):
            col_types[col] = String(10)     # bool como texto 'True'/'False'
        elif isinstance(sample, int):
            col_types[col] = BigInteger()
        elif isinstance(sample, float):
            col_types[col] = DOUBLE()       # Float() en MySQL es precision simple
        elif isinstance(sample, datetime):
            col_types[col] = DATETIME(fsp=6)
        elif isinstance(sample, date):
            col_types[col] = Date()
        else:
            # String - dos modos segun si nvarchar_max_length esta definido:
            #
            # CON nvarchar_max_length: todas las columnas string usan ese tamanio fijo.
            #   No se consulta el schema origen ni se infiere desde el chunk.
            #   Garantiza consistencia y evita truncamientos inesperados.
            #
            # SIN nvarchar_max_length: intenta inferir en este orden:
            #   1. Largo del schema origen (HANA, Oracle, Postgres)
            #   2. Largo maximo medido en el primer chunk
            #   3. LONGTEXT como ultimo recurso

            if nvarchar_max_length:
                # Tamanio fijo para todas las columnas string
                col_types[col] = _string_type(nvarchar_max_length)
            elif col_lengths and col in col_lengths and col_lengths[col]:
                # Schema origen disponible
                col_types[col] = _string_type(col_lengths[col])
            else:
                # Inferir desde el chunk
                measured = max(
                    (len(str(v)) for v in non_null),
                    default=0,
                )
                col_types[col] = _string_type(_next_width_tier(measured))

    return _fit_row_size(col_types)


def _ensure_table(
    engine: Engine,
    schema: str,
    table_name: str,
    chunk: list[dict],
    col_lengths: Optional[dict] = None,
    nvarchar_max_length: Optional[int] = None,
) -> None:
    """
    Crea la tabla en destino si no existe.
    Si ya existe, agrega columnas nuevas con ALTER TABLE.
    """
    # MySQL no distingue mayusculas en nombres de columna: 'EMAIL' y 'email'
    # chocan al crear la tabla. Fallar con un mensaje claro antes del CREATE.
    seen, clashes = {}, []
    for col in chunk[0]:
        key = col.lower()
        if key in seen:
            clashes.append((seen[key], col))
        seen[key] = col
    if clashes:
        raise ValueError(
            f"Columnas que solo difieren en mayusculas para '{schema}.{table_name}': {clashes}. "
            f"MySQL las considera iguales. Renombra una con alias en la query "
            f"o con 'rename_columns' en tables.json."
        )

    col_types = _infer_column_types(chunk, col_lengths, nvarchar_max_length)

    with engine.connect() as conn:
        exists = engine.dialect.has_table(conn, table_name, schema=schema)

    if not exists:
        metadata = MetaData()
        columns  = [
            Column(col_name, col_type, nullable=True)
            for col_name, col_type in col_types.items()
        ]
        Table(table_name, metadata, *columns, schema=schema, **TABLE_KWARGS)
        metadata.create_all(engine)
        logger.info("Tabla '%s.%s' creada con %d columnas.", schema, table_name, len(columns))
    else:
        with engine.connect() as conn:
            existing = {
                row[0] for row in conn.execute(
                    text(
                        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table"
                    ),
                    {"schema": schema, "table": table_name},
                )
            }

        new_columns = {k: v for k, v in col_types.items() if k not in existing}
        if new_columns:
            with engine.begin() as conn:
                for col_name, col_type in new_columns.items():
                    type_str = col_type.compile(dialect=engine.dialect)
                    try:
                        conn.execute(
                            text(
                                f"ALTER TABLE {_qualified(schema, table_name)} "
                                f"ADD COLUMN {_q(col_name)} {type_str} NULL"
                            )
                        )
                    except Exception as exc:
                        # Un VARCHAR nuevo puede no entrar en el limite de fila
                        if not isinstance(col_type, String) or isinstance(col_type, LONGTEXT):
                            raise
                        logger.warning(
                            "Columna '%s' no entro como %s (%s). Se agrega como LONGTEXT.",
                            col_name, type_str, exc,
                        )
                        type_str = "LONGTEXT"
                        conn.execute(
                            text(
                                f"ALTER TABLE {_qualified(schema, table_name)} "
                                f"ADD COLUMN {_q(col_name)} {type_str} NULL"
                            )
                        )
                    logger.info(
                        "Columna '%s' (%s) agregada a '%s.%s'.",
                        col_name, type_str, schema, table_name,
                    )


def _next_width_tier(measured: int) -> Optional[int]:
    """
    Devuelve el siguiente tamanio estandar que cubre el largo medido.
    Redondear a niveles evita hacer un ALTER por cada byte de crecimiento.
    None significa LONGTEXT.
    """
    for tier in (255, 500, 1000, 2000, 4000):
        if measured <= tier:
            return tier
    return None  # LONGTEXT


def get_string_column_widths(engine: Engine, schema: str, table_name: str) -> dict:
    """
    Devuelve el ancho actual de las columnas VARCHAR/CHAR de la tabla destino.

    {col_name: largo}. Las columnas TEXT/LONGTEXT no se incluyen porque
    no hay nada que ensanchar; tampoco las numericas y fechas.
    """
    query = text(
        "SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table "
        "AND DATA_TYPE IN ('varchar','char')"
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(query, {"schema": schema, "table": table_name}).fetchall()
        return {c: length for c, length in rows}
    except Exception as exc:
        logger.warning(
            "No se pudo consultar el ancho de columnas de '%s.%s': %s",
            schema, table_name, exc,
        )
        return {}


def widen_columns_if_needed(
    engine: Engine,
    schema: str,
    table_name: str,
    chunk: list[dict],
    current_widths: dict,
) -> None:
    """
    Ensancha con ALTER TABLE las columnas string que no alcanzan para el chunk.

    Evita el error 'Data too long for column' cuando un chunk trae valores
    mas largos que los del chunk que creo la tabla.
    Ensanchar nunca pierde datos, asi que es seguro hacerlo automaticamente.

    Si el VARCHAR mas ancho no entra en el limite de fila de MySQL,
    se reintenta como LONGTEXT.

    Muta current_widths con los nuevos anchos para no repetir el ALTER.
    """
    for col, current in list(current_widths.items()):
        if current is None:
            continue  # ya es LONGTEXT, no hay nada que ensanchar

        values = [row.get(col) for row in chunk]
        measured = max(
            (len(str(v)) for v in values if v is not None),
            default=0,
        )
        if measured <= current:
            continue

        new_width = _next_width_tier(measured)
        attempts  = [new_width] if new_width is None else [new_width, None]

        for width in attempts:
            type_str = _string_type_sql(width)
            try:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            f"ALTER TABLE {_qualified(schema, table_name)} "
                            f"MODIFY COLUMN {_q(col)} {type_str} NULL"
                        )
                    )
                current_widths[col] = width
                logger.info(
                    "Columna '%s.%s.%s' ensanchada de VARCHAR(%s) a %s "
                    "(el dato requeria %d caracteres).",
                    schema, table_name, col, current, type_str, measured,
                )
                break
            except Exception as exc:
                logger.warning(
                    "No se pudo ensanchar la columna '%s' de '%s.%s' a %s: %s",
                    col, schema, table_name, type_str, exc,
                )


def _truncate_table(engine: Engine, schema: str, table_name: str) -> None:
    """Trunca la tabla destino antes de la primera insercion."""
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {_qualified(schema, table_name)}"))
    logger.info("Tabla '%s.%s' truncada.", schema, table_name)


def validate_no_duplicate_columns(columns: list, source_desc: str) -> None:
    """
    Verifica que no haya nombres de columna repetidos.

    dict(zip(columns, row)) colapsa silenciosamente las columnas duplicadas
    y se pierde una sin aviso. Pasa cuando la query hace
    'SELECT a.id, b.id FROM a JOIN b' sin alias.
    Es mejor fallar con un mensaje claro que cargar datos incompletos.
    """
    seen, dupes = set(), []
    for c in columns:
        if c in seen and c not in dupes:
            dupes.append(c)
        seen.add(c)

    if dupes:
        raise ValueError(
            f"La consulta a '{source_desc}' devuelve columnas duplicadas: {dupes}. "
            f"Cargar asi perderia datos silenciosamente. "
            f"Agrega alias en la query, por ejemplo: SELECT a.id AS a_id, b.id AS b_id"
        )


def _normalize_value(value: Any) -> Any:
    """
    Convierte tipos de numpy/pandas a tipos Python nativos.

    pandas devuelve numpy.int64, numpy.float64, numpy.bool_, pd.Timestamp, etc.
    El driver no sabe escapar esos tipos. Esta funcion los normaliza.

    Tambien convierte NaN / NaT / pd.NA a None para que lleguen como NULL,
    y los bool a 'True'/'False' porque se guardan en columnas de texto.
    """
    if value is None:
        return None

    # NaN, NaT, pd.NA - detectar sin importar pandas si no hace falta
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except (ImportError, ValueError, TypeError):
        # ValueError/TypeError: pd.isna falla con arrays o tipos raros
        pass

    # numpy scalars exponen .item() que devuelve el tipo Python nativo
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            value = value.item()
        except (AttributeError, ValueError):
            pass

    if isinstance(value, bool):
        return str(value)

    return value


def write_chunks(
    engine: Engine,
    schema: str,
    table_name: str,
    chunks: Iterator[list[dict]],
    col_lengths: Optional[dict] = None,
    nvarchar_max_length: Optional[int] = None,
) -> int:
    """
    Escribe chunks de datos en la tabla destino.
    - Primer chunk: verifica/crea tabla, TRUNCATE, INSERT
    - Chunks siguientes: INSERT directo

    Args:
        engine:              Engine SQLAlchemy del destino.
        schema:              Base de datos destino en MySQL (ej: "raw").
        table_name:          Nombre de la tabla destino.
        chunks:              Iterable de listas de dicts.
        col_lengths:         Largos de columnas del schema origen (opcional).
        nvarchar_max_length: Tope maximo para strings inferidos (opcional).

    Returns:
        Total de filas insertadas.
    """
    total_rows     = 0
    first_chunk    = True
    current_widths = {}

    for chunk in chunks:
        if not chunk:
            continue

        # Normalizar tipos de numpy/pandas a Python nativo.
        chunk = [
            {k: _normalize_value(v) for k, v in row.items()}
            for row in chunk
        ]

        if first_chunk:
            _ensure_table(engine, schema, table_name, chunk, col_lengths, nvarchar_max_length)
            _truncate_table(engine, schema, table_name)
            # Cachear el ancho actual de las columnas string para poder
            # ensancharlas si un chunk trae valores mas largos
            current_widths = get_string_column_widths(engine, schema, table_name)
            first_chunk = False

        # Ensanchar columnas que no alcancen para este chunk
        if current_widths:
            widen_columns_if_needed(engine, schema, table_name, chunk, current_widths)

        cols    = list(chunk[0].keys())
        col_str = ", ".join(_q(c) for c in cols)
        val_str = ", ".join("%s" for _ in cols)
        sql     = f"INSERT INTO {_qualified(schema, table_name)} ({col_str}) VALUES ({val_str})"

        # Pasar los valores tal cual vienen del origen, sin convertir a string.
        # PyMySQL agrupa executemany en INSERTs multi-fila, que es lo rapido.
        rows_as_tuples = [
            tuple(row[c] for c in cols)
            for row in chunk
        ]

        with engine.begin() as conn:
            cursor = conn.connection.cursor()
            try:
                cursor.executemany(sql, rows_as_tuples)
            finally:
                cursor.close()

        total_rows += len(chunk)
        logger.debug(
            "Insertadas %d filas en '%s.%s'. Total: %d",
            len(chunk), schema, table_name, total_rows,
        )

    # El origen no devolvio ninguna fila.
    # Con semantica replace, RAW debe reflejar el origen: truncar la tabla.
    # Si no truncaramos, quedarian los datos del run anterior y la auditoria
    # reportaria exito con 0 filas - un falso positivo silencioso.
    if first_chunk:
        with engine.connect() as conn:
            exists = engine.dialect.has_table(conn, table_name, schema=schema)

        if exists:
            _truncate_table(engine, schema, table_name)
            logger.warning(
                "El origen no devolvio filas para '%s.%s'. La tabla quedo vacia "
                "(semantica replace). Verifica la query o el filtro de fecha.",
                schema, table_name,
            )
        else:
            logger.warning(
                "El origen no devolvio filas y la tabla '%s.%s' no existe. "
                "No se creo nada. Verifica la query o el filtro de fecha.",
                schema, table_name,
            )

    return total_rows
