"""
check_version.py
Verifica que correcciones tiene el build desplegado.
Util cuando un error que ya se corrigio vuelve a aparecer: casi siempre
significa que la maquina esta corriendo una version anterior.

Uso:
    python check_version.py
"""

from pathlib import Path

RAIZ = Path(__file__).parent

# (descripcion, archivo, texto que debe estar presente)
CORRECCIONES = [
    ("Destino MySQL",                   "sources/db_writer.py",   "mysql+pymysql"),
    ("Limite de fila MySQL",            "sources/db_writer.py",   "_fit_row_size"),
    ("Auditoria sin emoji",             "monitor/audit.py",       "[AUDIT] OK"),
    ("Salida forzada a UTF-8",          "run_pipeline.py",        'reconfigure(encoding="utf-8"'),
    ("Ensanchado automatico",           "sources/db_writer.py",   "widen_columns_if_needed"),
    ("Origen vacio trunca igual",       "sources/db_writer.py",   "semantica replace"),
    ("Validar columnas duplicadas",     "sources/db_writer.py",   "validate_no_duplicate_columns"),
    ("Normalizacion numpy/pandas",      "sources/db_writer.py",   "_normalize_value"),
    ("Formato de fecha configurable",   "sources/date_filter.py", "DATE_FORMATS"),
    ("Fechas flexibles en la CLI",      "sources/date_filter.py", "parse_date_input"),
    ("--source all",                    "run_pipeline.py",        "SOURCE_RUNNERS"),
    ("Origen SharePoint",               "sources/",               "sharepoint_utils.py"),
]


def main() -> None:
    print("Correcciones presentes en este build:")
    print("=" * 58)

    faltantes = 0
    for descripcion, ruta, marca in CORRECCIONES:
        objetivo = RAIZ / ruta
        if objetivo.is_dir():
            presente = (objetivo / marca).exists()
        else:
            presente = objetivo.exists() and marca in objetivo.read_text(encoding="utf-8")

        if not presente:
            faltantes += 1
        print(f"  [{'OK' if presente else '--'}]  {descripcion}")

    print("=" * 58)
    if faltantes:
        print(f"Faltan {faltantes} correccion(es). Descarga la ultima version del proyecto.")
    else:
        print("El build esta actualizado.")


if __name__ == "__main__":
    main()
