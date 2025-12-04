import hashlib
import uuid
import re
import psycopg2

# ==============================
# 1. DETERMINISTIC HASH
# ==============================
STATIC_SALT = "salto_estatico_2025_final"  # puedes cambiarlo, pero mantenlo fijo forever

def deterministic_hash(value: str) -> str:
    """
    Retorna un hash SHA256 estable del valor.
    Si el input es el mismo → el hash será siempre igual.
    """
    if value is None:
        return None

    v = str(value).strip().lower()
    return hashlib.sha256((v + STATIC_SALT).encode("utf-8")).hexdigest()


# ==============================
# 2. REDACTION
# ==============================
def redact(value: str, char: str = "*", keep_length: bool = False):
    """
    Oculta el texto.
    - keep_length=True → mantiene el número de caracteres
    """
    if value is None:
        return None

    if keep_length:
        return char * len(value)
    
    return "REDACTED"


# ==============================
# 3. PRESERVAR FORMATO DE TELÉFONO
# ==============================
def preserve_phone_format(phone: str):
    """
    Cambia los dígitos pero mantiene:
      - paréntesis
      - espacios
      - guiones
    """
    if phone is None:
        return None

    # Extraer todos los dígitos
    digits = re.sub(r"\D", "", phone)

    # Crear nuevos dígitos usando hash
    hashed = deterministic_hash(digits)
    new_digits = ''.join([str(int(c, 16) % 10) for c in hashed[:len(digits)]])

    # Insertar los nuevos dígitos en el formato original
    result = ""
    idx = 0
    for char in phone:
        if char.isdigit():
            result += new_digits[idx]
            idx += 1
        else:
            result += char

    return result


# ==============================
# 4. TOKENIZACIÓN (usa PostgreSQL)
# ==============================
def get_or_create_token(conn, original_id: str):
    """
    Si el ID ya existe, regresa el token.
    Si no existe, crea uno nuevo, lo guarda en token_vault y lo regresa.
    """
    if original_id is None:
        return None

    cursor = conn.cursor()

    # 1. Buscar si ya existe
    cursor.execute(
        "SELECT token_uuid FROM token_vault WHERE original_id = %s",
        (original_id,)
    )
    row = cursor.fetchone()

    if row:
        return row[0]  # token existente

    # 2. Si no existe, crearlo
    new_token = str(uuid.uuid4())

    cursor.execute(
        "INSERT INTO token_vault (original_id, token_uuid) VALUES (%s, %s)",
        (original_id, new_token)
    )
    conn.commit()

    return new_token
