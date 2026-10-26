"""
monitor/audit.py
Registro de ejecuciones del pipeline.
- Loguea siempre por consola.
- Opcionalmente persiste en pipeline_runs (MySQL, base destino).
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

# Se importa antes que db_writer en los pipelines: cargar .env aca tambien
load_dotenv()

logger = logging.getLogger(__name__)

# Tabla de auditoria en la base destino (la de DESTINATION_DATABASE).
# Con AUDIT_DATABASE se puede mandar a otra base del mismo servidor.
_AUDIT_TABLE = (
    f"`{os.environ['AUDIT_DATABASE']}`.pipeline_runs"
    if os.environ.get("AUDIT_DATABASE") else "pipeline_runs"
)

_CREATE_AUDIT_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {_AUDIT_TABLE} (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_name   VARCHAR(200)   NOT NULL,
    source_name     VARCHAR(100)   NOT NULL,
    table_name      VARCHAR(200)   NOT NULL,
    started_at      DATETIME(6)    NOT NULL,
    finished_at     DATETIME(6),
    status          VARCHAR(20)    NOT NULL,   -- 'success' | 'error' | 'running'
    rows_loaded     BIGINT,
    error_message   TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_INSERT_RUN_SQL = f"""
INSERT INTO {_AUDIT_TABLE}
    (pipeline_name, source_name, table_name, started_at, finished_at,
     status, rows_loaded, error_message)
VALUES
    (:pipeline_name, :source_name, :table_name, :started_at, :finished_at,
     :status, :rows_loaded, :error_message)
"""


@dataclass
class PipelineRun:
    """Representa una ejecucion individual de una tabla en el pipeline."""
    pipeline_name: str
    source_name:   str
    table_name:    str
    started_at:    datetime = field(default_factory=datetime.now)
    finished_at:   Optional[datetime] = None
    status:        str = "running"
    rows_loaded:   Optional[int] = None
    error_message: Optional[str] = None

    def complete(self, rows_loaded: int) -> None:
        self.finished_at = datetime.now()
        self.status      = "success"
        self.rows_loaded = rows_loaded

    def fail(self, error: str) -> None:
        self.finished_at  = datetime.now()
        self.status       = "error"
        self.error_message = str(error)[:2000]  # truncar mensajes muy largos

    def log(self) -> None:
        """Imprime el resultado de la ejecucion en el logger."""
        duration = (
            (self.finished_at - self.started_at).total_seconds()
            if self.finished_at else None
        )
        if self.status == "success":
            logger.info(
                "[AUDIT] OK    %s | tabla: %s | filas: %s | duracion: %.1fs",
                self.source_name, self.table_name, self.rows_loaded, duration or 0,
            )
        else:
            logger.error(
                "[AUDIT] ERROR %s | tabla: %s | error: %s | duracion: %.1fs",
                self.source_name, self.table_name, self.error_message, duration or 0,
            )


def ensure_audit_table(engine) -> None:
    """
    Crea la tabla 'pipeline_runs' si no existe.
    Llamar una sola vez al inicio del pipeline.
    """
    from sqlalchemy import text
    try:
        with engine.begin() as conn:
            conn.execute(text(_CREATE_AUDIT_TABLE_SQL))
        logger.info("Tabla de auditoria %s verificada/creada.", _AUDIT_TABLE)
    except Exception as exc:
        logger.warning("No se pudo crear la tabla de auditoria: %s", exc)


def save_run_to_db(run: PipelineRun, engine) -> None:
    """
    Persiste un PipelineRun en pipeline_runs.
    Si falla, solo loguea el error (no interrumpe el pipeline).
    """
    from sqlalchemy import text
    try:
        with engine.begin() as conn:
            conn.execute(
                text(_INSERT_RUN_SQL),
                {
                    "pipeline_name": run.pipeline_name,
                    "source_name":   run.source_name,
                    "table_name":    run.table_name,
                    "started_at":    run.started_at,
                    "finished_at":   run.finished_at,
                    "status":        run.status,
                    "rows_loaded":   run.rows_loaded,
                    "error_message": run.error_message,
                },
            )
    except Exception as exc:
        logger.warning("No se pudo guardar auditoria en BD: %s", exc)
