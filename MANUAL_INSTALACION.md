# Manual de instalacion (SPAP)

Este documento explica como instalar y ejecutar el proyecto en Windows, macOS o Linux. Incluye los servicios principales (dashboard, bot de Telegram, monitor y worker).

## 1) Requisitos

- Python 3.9+ (recomendado 3.10+)
- pip actualizado
- Git (opcional, solo para clonar)
- Acceso a Internet (para fuentes externas y descarga de modelos)
- Opcional: Redis (para cola en tiempo real)

Nota Windows:
- Si pip falla compilando dependencias, instala "Microsoft C++ Build Tools".

## 2) Clonar el repositorio

```bash
git clone https://github.com/Cyopn/SPAP.git
cd SPAP
```

Si ya tienes el repo, solo entra al folder:

```bash
cd SPAP
```

## 3) Crear y activar entorno virtual

### Windows (PowerShell)

```powershell
python -m venv .venv
.
.venv\Scripts\Activate.ps1
```

Si PowerShell bloquea la activacion:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.
.venv\Scripts\Activate.ps1
```

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 4) Instalar dependencias

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 5) Configurar variables de entorno (.env)

Crea o edita el archivo .env en la raiz del proyecto. Ejemplo (usa tus propios valores):

```env
BOT_TOKEN=
NEWS_API=
X_BEARER_TOKEN=
YOUTUBE_API_KEY=
WEB_HOST=127.0.0.1
WEB_PORT=5000
FLASK_SECRET=dev_secret_cambia_esto
REDIS_URL=redis://127.0.0.1:6379/0
PA_REALTIME_QUEUE_KEY=pa:realtime:queue
PORT=5001
```

Descripcion rapida:
- BOT_TOKEN: requerido para el bot de Telegram.
- NEWS_API: opcional, necesario para NewsAPI.
- X_BEARER_TOKEN / YOUTUBE_API_KEY: opcionales para X y YouTube.
- WEB_HOST / WEB_PORT: host y puerto del dashboard.
- FLASK_SECRET: secreto para sesiones web.
- REDIS_URL / PA_REALTIME_QUEUE_KEY: cola en tiempo real (Redis).
- PORT: puerto del listener de /webhook (web/realtime.py).

Nota: no subas .env con credenciales reales a git.

## 6) Primer arranque (opcion recomendada)

Ejecuta todos los servicios en una sola consola:

```bash
python run_all.py
```

Esto inicia:
- Dashboard web (Flask)
- Bot de Telegram (polling)
- Monitor automatico

## 7) Arranque por servicio

### 7.1 Dashboard web

```bash
python -m web.app
```

Accede a:
- http://WEB_HOST:WEB_PORT/

### 7.2 Bot de Telegram (polling)

```bash
python -m bots.telegram_bot
```

Si el bot no recibe mensajes, puede tener un webhook activo. Desactivalo:

```bash
python webhook.py
```

El bot usa el archivo telegram_offset.txt para mantener el offset. Si deseas reiniciar el polling, deten el bot y elimina ese archivo.

### 7.3 Monitor automatico

```bash
python -m monitors.monitor
```

El monitor lee su configuracion desde la base de datos (monitor_config). Puedes editarla desde el dashboard (seccion Config).

### 7.4 Worker en tiempo real (Redis)

Requiere Redis activo:

```bash
python -m workers.worker
```

Si Redis no esta disponible, el worker queda en espera. Sin Redis, los eventos en tiempo real se guardan directo en la BD.

### 7.5 Webhook de ingesta en tiempo real

```bash
python -m web.realtime
```

Envio de prueba:

```bash
curl -X POST http://127.0.0.1:5001/webhook \
  -H "Content-Type: application/json" \
  -d "{\"title\":\"Noticia\",\"summary\":\"Texto\",\"url\":\"https://ejemplo.com\"}"
```

## 8) Base de datos local

La base SQLite se crea automaticamente como pa_feed.db en la carpeta del proyecto. Si quieres reiniciar todo el historial:

1) Deten los servicios
2) Elimina pa_feed.db
3) Vuelve a iniciar

## 9) Descarga de modelos (NLP)

- spaCy: al usar el extractor completo, se descargara en_core_web_sm automaticamente.
- Transformers (Pegasus/T5): la primera ejecucion descarga modelos grandes. Requiere espacio en disco y tiempo.

## 10) Verificacion rapida

1) Abre el dashboard y verifica que cargue el feed.
2) Envia un mensaje al bot de Telegram y valida que aparezca en el feed.
3) Activa el monitor y revisa que agregue nuevas noticias.

## 11) Problemas comunes

- Error de dependencias en Windows: instala Microsoft C++ Build Tools.
- El bot no recibe mensajes: ejecuta python webhook.py para borrar webhook.
- Puerto ocupado: cambia WEB_PORT o PORT en .env.
- Redis no disponible: inicia Redis o desactiva el worker.

Si necesitas agregar configuraciones avanzadas (fuentes, horarios, alertas), usa la pagina Config del dashboard.
