# src/main.py
import os
import json
import psycopg2
from psycopg2.extras import execute_values
from tenacity import retry, wait_exponential, wait_fixed, stop_after_attempt
from src.config_loader import load_config
from src.db.connection import get_connection
from src.audit.auditor import Auditor

from src.masking.mask_utils import (
    deterministic_hash,
    redact,
    preserve_phone_format,
    get_or_create_token
)


# ===========================================
# ARCHIVO DE ESTADO (watermarks)
# ===========================================
STATE_FILE = "state/last_run.json"


def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=4, default=str)


# ===========================================
# APLICAR MASKING A UNA FILA
# ===========================================
def apply_masking(row: dict, rules: dict, conn_dest):
    new_row = row.copy()

    for col, rule in rules.items():
        if rule == "deterministic_hash":
            new_row[col] = deterministic_hash(row.get(col))
        elif rule in ["redaction", "redact"]:
            new_row[col] = redact(row.get(col))
        elif rule in ["preserve_format", "preserve_phone_format"]:
            new_row[col] = preserve_phone_format(row.get(col))
        elif rule == "tokenize":
            value = row.get(col)
            new_row[col] = get_or_create_token(conn_dest, str(value)) if value is not None else None
        else:
            # si no hay regla, conservar el valor original
            new_row[col] = row.get(col)

    return new_row


# ===========================================
# EXTRACT POR BATCHES
# ===========================================

@retry(
    wait=wait_fixed(1),
    stop=stop_after_attempt(3)
)
def extract_rows(cursor, table_name, batch_size=500, filter_clause=None):
    offset = 0
    while True:
        query = f"SELECT * FROM {table_name}"
        if filter_clause:
            query += f" WHERE {filter_clause}"
        query += f" LIMIT {batch_size} OFFSET {offset}"

        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            break

        colnames = [desc[0] for desc in cursor.description]
        batch = [dict(zip(colnames, r)) for r in rows]

        yield batch

        offset += batch_size


# ===========================================
# INSERT POR BATCH (execute_values)
# ===========================================
@retry(
    wait=wait_exponential(multiplier=1, min=1, max=5),
    stop=stop_after_attempt(5)
)
def insert_rows(conn, table_name, rows: list):
    """
    Inserta un batch. Reintenta automáticamente si falla.
    """
    if not rows:
        return

    cursor = conn.cursor()

    try:
        columns = list(rows[0].keys())
        col_str = ", ".join(columns)

        values_list = [[row[col] for col in columns] for row in rows]

        insert_query = f"INSERT INTO {table_name} ({col_str}) VALUES %s"

        execute_values(cursor, insert_query, values_list)

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


# ===========================================
# PIPELINE PRINCIPAL
# ===========================================
def run_pipeline(config_path="configs/config.example.json"):
    cfg = load_config(config_path)

    # Conexiones
    conn_src = get_connection(cfg["source_db"])
    conn_dst = get_connection(cfg["dest_db"])

    # Auditor (usa la conexión destino para persistir la auditoría)
    auditor = Auditor(conn_dst, cfg.get("env_name", "dev"))

    try:
        cursor_src = conn_src.cursor()
        print("=== EJECUTANDO PIPELINE ===")

        # Cargar watermarks
        state = load_state()

        for table in cfg["tables"]:
            table_name = table["name"]
            batch_size = table.get("batch_size", 500)

            print(f"\nProcesando tabla: {table_name}")

            # Determinar filtro incremental (watermark)
            last_wm = state.get(table_name)
            if last_wm:
                filter_clause = f"updated_at > '{last_wm}'"
            else:
                filter_clause = table.get("filter")

            total_rows_table = 0

            # Procesamiento por batches
            for batch in extract_rows(cursor_src, table_name, batch_size, filter_clause):
                print(f"  Batch extraído: {len(batch)} filas")

                try:
                    rules = cfg.get("masking_rules", {}).get(table_name, {})
                    masked_batch = [apply_masking(r, rules, conn_dst) for r in batch]

                    insert_rows(conn_dst, table_name, masked_batch)
                    print(f"  Batch insertado en {table_name}: {len(masked_batch)} filas")

                    # ⭐ REGISTRO DE AUDITORÍA (por batch)
                    auditor.log_table(table_name, len(masked_batch))
                    total_rows_table += len(masked_batch)

                    # ⭐ ACTUALIZAR WATERMARK (usar el máximo updated_at del batch)
                    # Asegurarse de que la columna exista y no sea None
                    updated_vals = [row.get("updated_at") for row in batch if row.get("updated_at") is not None]
                    if updated_vals:
                        new_wm = max(updated_vals)
                        # Guardar como cadena ISO para persistir en JSON
                        state[table_name] = str(new_wm)
                        save_state(state)
                except Exception as batch_error:
                    print(f"  ❌ Error al procesar batch en {table_name}: {batch_error}")
                    auditor.log_error(table_name, str(batch_error))
                    # Continuar con el siguiente batch en lugar de parar todo
                    continue

            if total_rows_table == 0:
                print(f"  No se procesaron filas para {table_name}")

        print("\n=== PIPELINE COMPLETADO ===")

    except Exception as e:
        # Registrar error genérico en auditoría
        try:
            auditor.log_error("general", str(e))
        except Exception:
            pass
        # Re-lanzar o imprimir para visibilidad
        print("ERROR EN EL PIPELINE:", e)

    finally:
        # Siempre intentar escribir auditoría y cerrar conexiones
        try:
            auditor.finish()
        except Exception as ex:
            print("ERROR al guardar auditoría:", ex)

        try:
            conn_src.close()
        except Exception:
            pass
        try:
            conn_dst.close()
        except Exception:
            pass


if __name__ == "__main__":
    run_pipeline()
