"""
sources/csv_utils.py
Lectura de archivos CSV y TXT por chunks usando pandas.
Soporta cualquier delimitador y encoding configurable desde el JSON.
"""

import fnmatch
import logging
import os
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 10_000

# Columnas de trazabilidad que se agregan cuando file_metadata=True
METADATA_COLUMNS = ("file_path", "file_name", "file_number_register")

ENCODING_ALIASES = {
    "latin":     "latin-1",
    "iso":       "latin-1",
    "ansi":      "latin-1",
    "utf8":      "utf-8",
    "utf-8-bom": "utf-8-sig",
}


def _resolve_encoding(encoding: str) -> str:
    """Normaliza el nombre del encoding para evitar errores comunes."""
    return ENCODING_ALIASES.get(encoding.lower(), encoding.lower())


def _resolve_delimiter(delimiter: str) -> str:
    """
    Convierte representaciones de texto del delimitador al caracter real.
    Permite poner 'tab' o '\\t' en el JSON para archivos TSV.
    """
    if delimiter in ("\\t", "tab", "TAB"):
        return "\t"
    return delimiter


def resolve_source_files(folder: str, mask: str, latest_file_only: bool = False) -> list[Path]:
    """
    Devuelve los archivos de 'folder' cuyo nombre coincide con 'mask'.

    La mascara acepta comodines (* y ?), ej: 'Reporte_TotalAfiliados_*.csv'.
    En Windows la comparacion no distingue mayusculas.

    Args:
        folder:           Carpeta donde buscar.
        mask:             Nombre exacto o mascara del archivo.
        latest_file_only: Si True, devuelve solo el primero en orden alfabetico
                          descendente (con fecha en el nombre, el mas reciente).

    Returns:
        Lista de rutas en orden alfabetico ascendente (o un solo elemento).
    """
    base = Path(folder)
    if not base.is_dir():
        raise FileNotFoundError(
            f"No se encontro la carpeta: '{folder}'. "
            f"Verifica 'source_folder' en config/tables.json."
        )

    files = sorted(
        (base / name for name in os.listdir(base)
         if fnmatch.fnmatch(name, mask) and (base / name).is_file()),
        key=lambda p: p.name,
    )
    if not files:
        raise FileNotFoundError(
            f"No hay archivos que coincidan con '{mask}' en '{folder}'."
        )

    if latest_file_only:
        return [files[-1]]
    return files


def read_csv_chunks(
    file_path: str,
    delimiter: str,
    encoding: str,
    columns: Optional[list[str]] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    all_string: bool = False,
    rename_columns: Optional[dict] = None,
    file_metadata: bool = False,
) -> Iterator[list[dict]]:
    """
    Lee un archivo CSV o TXT y devuelve los datos por chunks como listas de dicts.
    Siempre asume que la primera fila es el encabezado.

    Args:
        file_path:  Ruta completa al archivo (.csv, .txt, .tsv, etc.).
        delimiter:  Separador de columnas (';', ',', 'tab', '|', etc.).
        encoding:   Encoding del archivo ('utf-8', 'latin-1', 'utf-8-sig', etc.).
        columns:    Columnas a extraer. None = todas.
        chunk_size: Filas por chunk.
        all_string: Si True, fuerza todo a string. Si False, pandas infiere tipos.
        rename_columns: {nombre_original: nombre_destino}. Se aplica despues
                    de 'columns', asi que 'columns' usa los nombres del archivo.
        file_metadata: Si True, agrega al final file_path (carpeta), file_name
                    y file_number_register (numero de registro en el archivo,
                    empezando en 1 sin contar el encabezado).

    Yields:
        Lista de dicts con las filas del chunk actual.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(
            f"No se encontro el archivo: '{file_path}'. "
            f"Verifica la ruta en config/tables.json."
        )

    delimiter_real = _resolve_delimiter(delimiter)
    encoding_real  = _resolve_encoding(encoding)
    dtype          = str if all_string else None

    logger.debug(
        "Leyendo CSV: %s | delimitador: %r | encoding: %s | all_string: %s",
        file_path, delimiter_real, encoding_real, all_string,
    )

    try:
        df = pd.read_csv(
            file_path,
            sep=delimiter_real,
            encoding=encoding_real,
            dtype=dtype,
            header=0,
            keep_default_na=True,
            engine="python",
        )
    except UnicodeDecodeError:
        raise ValueError(
            f"Error de encoding al leer '{file_path}'. "
            f"El encoding '{encoding}' no es correcto para este archivo. "
            f"Encodings comunes: utf-8, latin-1, utf-8-sig"
        )

    if columns:
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(
                f"Columnas no encontradas en '{file_path}': {missing}. "
                f"Disponibles: {list(df.columns)}"
            )
        df = df[columns]

    if rename_columns:
        missing = [c for c in rename_columns if c not in df.columns]
        if missing:
            raise ValueError(
                f"rename_columns referencia columnas que no existen en '{file_path}': {missing}."
            )
        df = df.rename(columns=rename_columns)

    if file_metadata:
        clashes = [c for c in df.columns if str(c).lower() in METADATA_COLUMNS]
        if clashes:
            raise ValueError(
                f"El archivo '{file_path}' ya trae columnas {clashes}, que chocan con "
                f"las columnas de trazabilidad. Renombralas con 'rename_columns'."
            )
        # Se numera antes del dropna para que el numero refleje la posicion en el archivo
        df["file_path"]            = str(path.parent)
        df["file_name"]            = path.name
        df["file_number_register"] = range(1, len(df) + 1)

    df = df.dropna(how="all")
    total_rows = len(df)
    logger.debug("CSV '%s': %d filas encontradas.", path.name, total_rows)

    for start in range(0, total_rows, chunk_size):
        chunk_df = df.iloc[start:start + chunk_size]
        yield chunk_df.where(pd.notna(chunk_df), other=None).to_dict(orient="records")
