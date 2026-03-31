import os
import requests
from dotenv import load_dotenv
from pathlib import Path
from core.logger import log, log_exc

env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()
t = os.environ.get('BOT_TOKEN')
try:
    if not t:
        raise KeyError('BOT_TOKEN not set')
    info = requests.get(
        f"https://api.telegram.org/bot{t}/getWebhookInfo").json()
    log(f"webhook: getWebhookInfo: {info}", "INFO")
    deleted = requests.post(
        f"https://api.telegram.org/bot{t}/deleteWebhook").json()
    log(f"webhook: deleteWebhook: {deleted}", "INFO")
except Exception as e:
    log_exc("webhook: error contacting Telegram API", e)
