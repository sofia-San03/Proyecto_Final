import json
import os

def load_config(path="configs/config.example.json"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # Cargar passwords desde variables de entorno
    for key in ("source_db", "dest_db"):
        pw_env = cfg[key].get("password_env")
        if pw_env:
            cfg[key]["password"] = os.environ.get(pw_env)

    return cfg

if __name__ == "__main__":
    cfg = load_config()
    print("===== CONFIG CARGADO =====")
    print("Entorno:", cfg["env_name"])
    print("Host origen:", cfg["source_db"]["host"])
    print("Host destino:", cfg["dest_db"]["host"])
