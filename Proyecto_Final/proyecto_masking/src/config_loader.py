import json
import os


def load_local_env(env_file=".env.local"):
    """
    Carga variables de entorno desde un archivo tipo .env.local
    (formato: CLAVE=VALOR, líneas con # se ignoran).
    Solo establece la variable si NO existe ya en el entorno.
    """
    if not os.path.exists(env_file):
        return

    with open(env_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            os.environ.setdefault(key, value)


def load_config(path="configs/config.example.json"):
    # Primero cargamos variables desde .env.local (si existe)
    load_local_env()

    # Ahora leemos el JSON de configuración
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # Cargar passwords desde variables de entorno
    for key in ("source_db", "dest_db"):
        pw_env = cfg[key].get("password_env")
        if pw_env:
            cfg[key]["password"] = os.environ.get(pw_env)

    return cfg


if __name__ == "__main__":
    # Prueba rápida
    c = load_config()
    print("SRC password:", c["source_db"].get("password"))
    print("DST password:", c["dest_db"].get("password"))
