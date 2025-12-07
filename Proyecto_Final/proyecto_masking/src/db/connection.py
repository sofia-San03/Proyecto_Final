import os
import psycopg2


def get_connection(db_config):
    """
    Crea una conexión a PostgreSQL usando la información de db_config.

    Prioridad para obtener la contraseña:
    1. Si db_config["password"] existe, se usa directamente.
    2. En otro caso, si hay "password_env", se busca en os.environ.
    3. Si no se puede obtener, se lanza un ValueError.
    """
    # 1) Intentar usar la contraseña que ya puso load_config
    password = db_config.get("password")

    # 2) Si no hay password directo, intentar por variable de entorno
    if not password:
        pw_env = db_config.get("password_env")
        if pw_env:
            password = os.environ.get(pw_env)
            if not password:
                raise ValueError(f"No se encontró la variable de entorno: {pw_env}")
        else:
            raise ValueError("No se especificó 'password' ni 'password_env' en la configuración.")

    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=password,
        dbname=db_config["database"],
    )
    return conn
