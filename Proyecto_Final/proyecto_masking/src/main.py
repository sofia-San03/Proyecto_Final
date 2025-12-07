# src/main.py
import os
import json
import psycopg2
from psycopg2.extras import execute_values
from tenacity import retry, wait_exponential, wait_fixed, stop_after_attempt
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

from src.config_loader import load_config
from src.db.connection import get_connection
from src.audit.auditor import Auditor

from src.masking.mask_utils import (
    deterministic_hash,
    redact,
    preserve_phone_format,
    get_or_create_token,
)

# ===========================================
# CLAVES PRIMARIAS PARA UPSERT (IDEMPOTENCIA)
# ===========================================
UPSERT_KEYS = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["item_id"],
}

# ===========================================
# ARCHIVO DE ESTADO (watermarks) PARA DELTA
# ===========================================
STATE_FILE = "state/last_run.json"


def load_state():
    """
    Cargar marcas de agua desde state/last_run.json.
    Si no existe, regresamos {}.
    """
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    """
    Guardar marcas de agua por tabla en state/last_run.json.
    """
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=4, default=str)


# ===========================================
# TRUNCATE EN QA (para modo FULL) - opcional
# ===========================================
def truncate_table_in_qa(conn, table_name: str):
    """
    Elimina todo el contenido de la tabla destino en QA.
    Se usaría en modo FULL.
    """
    with conn.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {table_name};")
    conn.commit()
    print(f"  Tabla {table_name} truncada en QA")


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
            new_row[col] = (
                get_or_create_token(conn_dest, str(value)) if value is not None else None
            )
        else:
            # si no hay regla, conservar el valor original
            new_row[col] = row.get(col)

    return new_row


# ===========================================
# EXTRACT POR BATCHES
# ===========================================
@retry(wait=wait_fixed(1), stop=stop_after_attempt(3))
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
# INSERT POR BATCH (execute_values) con UPSERT
# ===========================================
@retry(
    wait=wait_exponential(multiplier=1, min=1, max=5),
    stop=stop_after_attempt(5),
)
def insert_rows(conn, table_name, rows: list):
    """
    Inserta un batch en QA.
    - Si la tabla está en UPSERT_KEYS, usa ON CONFLICT (idempotencia).
    - Si no, hace un INSERT normal.
    """
    if not rows:
        return

    cursor = conn.cursor()

    try:
        columns = list(rows[0].keys())
        col_str = ", ".join(columns)

        values_list = [[row[col] for col in columns] for row in rows]

        pk_cols = UPSERT_KEYS.get(table_name)

        if pk_cols:
            # Columnas de conflicto (PK)
            conflict_cols = ", ".join(pk_cols)
            # Columnas que se actualizarán (todas menos las PK)
            update_cols = [c for c in columns if c not in pk_cols]
            set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

            insert_query = (
                f"INSERT INTO {table_name} ({col_str}) VALUES %s "
                f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}"
            )
        else:
            # Tabla sin configuración de UPSERT: INSERT normal
            insert_query = f"INSERT INTO {table_name} ({col_str}) VALUES %s"

        execute_values(cursor, insert_query, values_list)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()


# ===========================================
# VALIDACIÓN DE ROL QUE EJECUTA EL PIPELINE
# ===========================================
def check_runner_role(conn, allowed_roles):
    """
    Verifica que el rol actual de la sesión de BD esté dentro de los
    roles permitidos. Si no, lanza un PermissionError.
    """
    if not allowed_roles:
        # Si la lista está vacía o no viene, no hacemos validación extra
        return

    with conn.cursor() as cur:
        cur.execute("SELECT current_user;")
        current_role = cur.fetchone()[0]

    if current_role not in allowed_roles:
        raise PermissionError(
            f"Rol de BD no autorizado para ejecutar el pipeline: '{current_role}'. "
            f"Solo se permiten: {', '.join(allowed_roles)}."
        )


# ===========================================
# PIPELINE PRINCIPAL (con paralelismo opcional)
# ===========================================
def run_pipeline(config_path="configs/config.example.json", mode_override=None):
    cfg = load_config(config_path)

    # Pedir modo al usuario si no viene forzado
    if mode_override is None:
        mode_input = input(
            "¿En qué modo quieres correr el pipeline? [delta/full] (default: delta): "
        ).strip().lower()
        mode = mode_input if mode_input in ("delta", "full") else "delta"
    else:
        mode = mode_override

    dry_run = cfg.get("dry_run", False)
    env_name = cfg.get("env_name", "dev_local")

    print(f"Entorno: {env_name}")
    print(f"Modo de ejecución: {mode}")
    print(f"Dry run: {dry_run}")

    # 🔌 Conexiones base (solo para validar y para el auditor)
    conn_src = get_connection(cfg["source_db"])
    conn_dst = get_connection(cfg["dest_db"])

    # 🔐 Validar rol que está corriendo el pipeline
    allowed_roles = cfg.get("allowed_runner_roles", [])
    try:
        check_runner_role(conn_dst, allowed_roles)
        print("Rol de BD autorizado, puedes ejecutar el pipeline.")
    except PermissionError as e:
        print("\n🚫 ERROR DE SEGURIDAD:", e)
        # Cerramos conexiones y salimos SIN seguir con el pipeline
        try:
            conn_src.close()
        except Exception:
            pass
        try:
            conn_dst.close()
        except Exception:
            pass
        return  # Salida temprana

    # Si pasa la validación, ahora sí creamos el auditor
    auditor = Auditor(conn_dst, env_name)

    try:
        print("=== EJECUTANDO PIPELINE ===")

        # Cargar watermarks y lock para acceso concurrente
        state = load_state()
        state_lock = threading.Lock()

        tables_cfg = cfg.get("tables", [])
        parallel_tables = cfg.get("parallel_tables", False)
        max_workers = cfg.get("max_workers", 3)

        print(
            f"Paralelismo por tabla: {parallel_tables} "
            f"(workers = {max_workers})"
        )

        # -------------------------
        # Función que procesa UNA tabla
        # -------------------------
        def process_table(table):
            table_name = table["name"]
            batch_size = table.get("batch_size", 500)

            print(f"\nProcesando tabla: {table_name}")
            print(f"  Tamaño de batch configurado: {batch_size}")

            table_start = time.perf_counter()

            # Conexiones LOCALES por thread
            conn_src_local = get_connection(cfg["source_db"])
            conn_dst_local = get_connection(cfg["dest_db"])
            cursor_src_local = conn_src_local.cursor()

            try:
                # Leer watermark con lock
                with state_lock:
                    last_wm = state.get(table_name)

                if last_wm and mode == "delta":
                    filter_clause = f"updated_at > '{last_wm}'"
                    print(f"  Usando watermark previo: updated_at > {last_wm}")
                else:
                    filter_clause = table.get("filter")
                    if mode == "delta" and not last_wm:
                        print(
                            "  No hay watermark previo, se toma todo "
                            "(primera ejecución delta)."
                        )

                total_rows_table = 0

                # Procesamiento por batches
                for batch in extract_rows(
                    cursor_src_local, table_name, batch_size, filter_clause
                ):
                    print(f"  Batch extraído: {len(batch)} filas")

                    try:
                        rules = cfg.get("masking_rules", {}).get(table_name, {})
                        masked_batch = [
                            apply_masking(r, rules, conn_dst_local) for r in batch
                        ]

                        if dry_run:
                            print(
                                f"  (Dry run) NO se insertan filas en QA "
                                f"({len(masked_batch)} filas)."
                            )
                        else:
                            insert_rows(conn_dst_local, table_name, masked_batch)
                            print(
                                f"  Batch insertado en {table_name}: "
                                f"{len(masked_batch)} filas"
                            )

                        auditor.log_table(table_name, len(masked_batch))
                        total_rows_table += len(masked_batch)

                        # Actualizar watermark solo en modo delta
                        if mode == "delta":
                            updated_vals = [
                                row.get("updated_at")
                                for row in batch
                                if row.get("updated_at") is not None
                            ]
                            if updated_vals:
                                new_wm = max(updated_vals)
                                with state_lock:
                                    state[table_name] = str(new_wm)
                                    save_state(state)

                    except Exception as batch_error:
                        print(
                            f"  ❌ Error al procesar batch en {table_name}: "
                            f"{batch_error}"
                        )
                        auditor.log_error(table_name, str(batch_error))
                        continue

                # Fin de la tabla: medimos tiempo
                table_elapsed = time.perf_counter() - table_start
                if total_rows_table == 0:
                    print(f"  No se procesaron filas para {table_name}")
                print(
                    f"⏱  Tiempo total tabla {table_name}: "
                    f"{table_elapsed:.3f} segundos"
                )
                print(
                    f"   Filas totales procesadas en {table_name}: "
                    f"{total_rows_table}"
                )

            finally:
                # Cerrar recursos locales
                try:
                    cursor_src_local.close()
                except Exception:
                    pass
                try:
                    conn_src_local.close()
                except Exception:
                    pass
                try:
                    conn_dst_local.close()
                except Exception:
                    pass

        # -------------------------
        # Ejecutar tablas secuencial o en paralelo
        # -------------------------
        if parallel_tables and len(tables_cfg) > 1:
            print("\n>>> Ejecutando tablas en paralelo...")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_table, t) for t in tables_cfg]
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        print("Error en thread de tabla:", e)
                        auditor.log_error("thread", str(e))
        else:
            # Modo clásico secuencial
            for table in tables_cfg:
                process_table(table)

        print("\n=== PIPELINE COMPLETADO ===")

    except Exception as e:
        # Registrar error genérico en auditoría
        try:
            auditor.log_error("general", str(e))
        except Exception:
            pass
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
    # Leer config solo para saber el modo por defecto
    cfg = load_config("configs/config.example.json")
    default_mode = cfg.get("mode", "delta")

    # Preguntar al usuario
    resp = input(
        f"¿En qué modo quieres correr el pipeline? "
        f"[delta/full] (default: {default_mode}): "
    ).strip().lower()

    if resp == "":
        chosen_mode = default_mode
    elif resp in ("delta", "full"):
        chosen_mode = resp
    else:
        print(
            f"Modo '{resp}' no válido, usando modo por defecto "
            f"'{default_mode}'."
        )
        chosen_mode = default_mode

    # Ejecutar pipeline con el modo elegido
    run_pipeline(mode_override=chosen_mode)
