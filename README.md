# 🛡️ Proyecto Final – Sistema de Enmascaramiento de Datos

Este repositorio contiene el proyecto **Sistema de Masking de Datos**, una herramienta diseñada para proteger información sensible al copiar datos desde ambientes de producción hacia ambientes de pruebas (QA).
Incluye las funciones de conexión a bases de datos, auditoría, enmascaramiento determinístico, tokenización y ejecución automática mediante pipelines.

Además, dentro del repositorio se encuentra un archivo **`.venv.zip`**, que contiene el entorno virtual listo para usarse.

---

## 📥 1. Cómo descargar el proyecto

### **Opción A — Descargar ZIP**

1. En la parte superior del repositorio, haz clic en el botón **Código (Code)**.
2. Selecciona **Download ZIP**.
3. Extrae el contenido en tu computadora.

### **Opción B — Clonar el repositorio (si tienes Git instalado)**

```bash
git clone https://github.com/tu_usuario/Proyecto_Final.git
```

---

## 🧰 2. Preparar el entorno virtual (`.venv`)

Dentro del repositorio viene un archivo comprimido:

```
.venv.zip
```

Este archivo contiene el entorno virtual con todas las dependencias instaladas.

### **Pasos para activar el entorno:**

### 🟦 **Windows**

1. Descomprime el archivo `.venv.zip` (clic derecho > Extraer aquí).
2. Verás una carpeta llamada `.venv/`.
3. Abre una terminal (CMD o PowerShell) dentro del proyecto.
4. Activa el entorno:

#### **PowerShell**

```powershell
.\.venv\Scripts\Activate.ps1
```

#### **CMD**

```cmd
.\.venv\Scripts\activate.bat
```

---

## ⚙️ 3. Configurar las variables de entorno

Antes de correr el proyecto, define las contraseñas de tus bases de datos:

### **PowerShell**

```powershell
$env:SRC_DB_PASS = "tu_password"
$env:DST_DB_PASS = "tu_password"
```

### **CMD**

```cmd
set SRC_DB_PASS=tu_password
set DST_DB_PASS=tu_password
```

---

## ▶️ 4. Ejecutar el proyecto

Una vez activado el entorno virtual y configuradas las variables, ejecuta:

```bash
python -m src.main
```

---

## 🗂️ Estructura del proyecto

```
Proyecto_Final/
│
├── proyecto_enmascaramiento/   # Código principal del sistema
├── .gitattributes
├── .venv.zip                   # Entorno virtual comprimido
└── README.md
```

---

## 📌 Notas importantes

* **No subas nuevamente la carpeta `.venv` descomprimida a GitHub**, solo el `.zip`.
* Si llegas a reinstalar dependencias, recuerda actualizar el entorno virtual localmente, **no desde GitHub**.

---
