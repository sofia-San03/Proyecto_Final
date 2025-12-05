import uuid
import json
import datetime
import psycopg2


class Auditor:
    def __init__(self, conn, env_name):
        self.conn = conn
        self.env_name = env_name
        self.execution_id = str(uuid.uuid4())
        self.start_time = datetime.datetime.now()
        self.tables_processed = []
        self.rows_copied = 0
        self.rows_failed = 0
        self.errors = []

    def log_table(self, table_name, rows_count):
        """
        Guardar información por tabla procesada.
        """
        self.tables_processed.append({
            "table": table_name,
            "rows": rows_count
        })
        self.rows_copied += rows_count

    def log_error(self, table_name, error_msg):
        """
        Registrar errores.
        """
        self.rows_failed += 1
        self.errors.append({
            "table": table_name,
            "error": str(error_msg)
        })

    def finish(self):
        """
        Registrar fin de ejecución en execution_audit.
        """
        end_time = datetime.datetime.now()
        cursor = self.conn.cursor()

        query = """
        INSERT INTO execution_audit (
            execution_id,
            started_at,
            finished_at,
            env_name,
            tables_processed,
            rows_copied,
            rows_failed,
            errors
        ) VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb)
        """

        cursor.execute(query, (
            self.execution_id,
            self.start_time,
            end_time,
            self.env_name,
            json.dumps(self.tables_processed),
            self.rows_copied,
            self.rows_failed,
            json.dumps(self.errors)
        ))

        self.conn.commit()
        print(f"\n📝 Auditoría guardada con ID: {self.execution_id}")
