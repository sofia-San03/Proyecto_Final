import os
import sys
from dotenv import load_dotenv
sys.path.append(os.path.abspath("."))

from src.config_loader import load_config
from tenacity import retry, wait_fixed, stop_after_attempt
import psycopg2

# Cargar variables de .env

load_dotenv()

@retry(
    wait=wait_fixed(2),         # espera 2 segundos entre intentos
    stop=stop_after_attempt(5)  # máximo 5 intentos
)

def get_connection(db_config: dict):
    """
    Recibe un diccionario con la forma:
    {
        "host": "...",
        "port": 5432,
        "user": "...",
        "password_env": "...",
        "database": "..."
    }
    Usa el nombre de la variable en 'password_env' para obtener la contraseña real del entorno.
    """
    password = os.environ.get(db_config["password_env"])
    if password is None:
        raise ValueError(f"No se encontró la variable de entorno: {db_config['password_env']}")

    print(f"Intentando conectar a: {db_config['database']} en {db_config['host']}:{db_config['port']}")
    
    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=password,
        dbname=db_config["database"]
    )
    
    print(f"Conexión exitosa a {db_config['database']} ✔")
    return conn


if __name__ == "__main__":
    print("=== Probando conexión a las bases de datos ===\n")

    # Cargar configuración
    cfg = load_config("configs/config.example.json")

    # Conexión a SOURCE
    try:
        conn_src = get_connection(cfg["source_db"])
        conn_src.close()
    except Exception as e:
        print("ERROR al conectar a SOURCE:", e)

    print("\n")  # separador

    # Conexión a DESTINO
    try:
        conn_dst = get_connection(cfg["dest_db"])
        conn_dst.close()
    except Exception as e:
        print("ERROR al conectar a DESTINO:", e)
