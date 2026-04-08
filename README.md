# Buscador de noticias (Python)

Script para buscar noticias a partir de palabras clave en:
- Motores de busqueda: Google News, Bing News

## Instalacion

```bash
pip install -r requirements.txt
```

## Uso basico

```bash
python news_finder.py --keywords "inteligencia artificial" "ciberseguridad"
```

## Opciones utiles

```bash
python news_finder.py \
  --keywords "energia solar" "economia" \
  --sources google bing newsapi \
  --limit 10 \
  --show 25 \
  --json-out noticias.json \
  --csv-out noticias.csv
```

## Soporte para redes sociales que requieren credenciales

El script incluye soporte opcional para X/Twitter y YouTube vía APIs oficiales. Estas fuentes requieren credenciales y permisos:

- X/Twitter: proporciona un `Bearer Token` (env `X_BEARER_TOKEN` o `--x-bearer-token`).
- YouTube: requiere API Key de YouTube Data API v3 (env `YOUTUBE_API_KEY`).

Ejemplo con X/Twitter:

```bash
export X_BEARER_TOKEN="TU_TOKEN"
python news_finder.py --keywords "inteligencia artificial" --sources google x --limit 5
```

Si una fuente requiere credenciales y no están disponibles, el script la omitirá mostrando un aviso.

## Notas

- Algunas fuentes pueden limitar peticiones por IP.
- Este script usa endpoints publicos (sin claves API), por lo que es ideal para prototipos.
- Si quieres incluir X/Twitter, TikTok o LinkedIn, normalmente necesitaras APIs oficiales y credenciales.

---

## Dashboard y Bot de Telegram (feed en tiempo real)

Este repositorio incluye un pequeño dashboard Flask y un bot de Telegram que permiten recibir, clasificar y publicar noticias/avisos en un feed en tiempo real.

Archivos principales:

- `app.py`: servidor Flask con endpoints de búsqueda y dos endpoints para el feed en tiempo real: `/live` (JSON) y `/stream` (SSE).
 - `telegram_bot.py`: poller simple que escucha mensajes entrantes, los clasifica y los guarda en la base SQLite (`pa_feed.db`) a través del módulo `storage`. También puede publicar en un chat objetivo.
- `classifier.py` y `classifier_config.json`: clasificador configurable por palabras clave que devuelve niveles `high`, `medium`, `low` con emoji y color.
- `templates/index.html`: UI que muestra resultados y el feed en tiempo real (actualiza vía SSE).

### Variables de entorno

Configura estas variables en `.env` (ya existe `BOT_TOKEN` en el repositorio de ejemplo):

- `BOT_TOKEN` (requerido) — token del bot de Telegram.
- `TELEGRAM_TARGET_CHAT_ID` (opcional) — chat_id o @username donde el bot publicará automáticamente.
- `X_BEARER_TOKEN`, `YOUTUBE_API_KEY` — opcionales para `news_finder.py`.

### Instalación

Instala dependencias:

```bash
pip install -r requirements.txt
```

### Ejecutar localmente

1) Arrancar el dashboard (Flask):

```bash
python app.py
```

El dashboard estará en http://127.0.0.1:5000 y contiene la sección "Feed en tiempo real" que se actualiza automáticamente.

2) Arrancar el bot de Telegram (polling):

```bash
python telegram_bot.py
```

El bot leerá mensajes enviados a él y realizará clasificación automática. También soporta comandos:

- `/classify <texto>` — clasifica el texto y lo añade al feed.
- `/level <high|medium|low>` — fuerza el nivel; si se usa en respuesta (reply) aplicará al mensaje respondido.

Ejemplos:

```text
/classify Hay un incendio en la zona central, ¡evacuar!
/level high (o) /level alta (en español)
```

### Cómo funciona la clasificación

El clasificador usa `classifier_config.json` con keywords y puntajes. Cada coincidencia suma puntos; si la suma supera umbrales, el nivel será `high`, `medium` o `low`. Puedes editar `classifier_config.json` para ajustar palabras y umbrales.

### Archivo de feed

El feed en tiempo real se guarda en la base de datos SQLite (`pa_feed.db`). El servidor lee los ítems desde `storage.read_items()` y los expone en `/live` y `/stream`.

### Integración con `news_finder.py`

Si quieres que resultados automáticos de `news_finder.py` se publiquen en el feed, puedo añadir una opción para que este script llame a la API local (`/live` o crear un endpoint `POST /ingest`) y así centralizar las noticias.

---

Si quieres, actualizo `README.md` con ejemplos de `curl` para el endpoint SSE, o implemento la ingesta automática desde `news_finder.py`.

### Monitorización automática y reportes

Puedes habilitar monitorización periódica editando `monitor_config.json` y luego ejecutando `python monitor.py`. El monitor:

- consulta las `keywords` configuradas en varias `sources` cada `interval_minutes`.
- clasifica cada resultado con `classifier.py` y añade ítems nuevos a la base de datos (`pa_feed.db`) vía `storage.append_item()`.
- envía alertas a Telegram para ítems `high` cuando `telegram_alerts` está activo.

Generar reportes:

```bash
# Reporte diario
python report_generator.py --period daily

# Reporte semanal
python report_generator.py --period weekly
```

Los reportes se guardan en la carpeta `reports/` por defecto.

### Iniciar todos los servicios con un comando

Puedes arrancar el servidor Flask, el bot de Telegram y el monitor con un único comando:

```bash
python run_all.py
```

El launcher ejecuta los tres procesos y muestra sus logs prefijados (`[flask]`, `[bot]`, `[monitor]`). Presiona `Ctrl+C` para detenerlos.
