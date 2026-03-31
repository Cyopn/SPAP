from __future__ import annotations

import os
from flask import Flask, jsonify, request, render_template, redirect, url_for, flash, Response, send_from_directory
import json
import time

from core import storage, classifier
from core.logger import log, log_exc
import importlib

_BASE_DIR = os.path.dirname(__file__)
_TEMPLATES_DIR = os.path.join(_BASE_DIR, "templates")

app = Flask(__name__, template_folder=_TEMPLATES_DIR)
app.secret_key = os.environ.get("FLASK_SECRET", "dev_secret")


@app.route("/api/items")
def api_items():
    items = storage.read_items(200)
    return jsonify(items)


@app.before_request
def _log_request_info():
    try:
        log(f"web: incoming request: {request.method} {request.path}", "DEBUG")
    except Exception:
        pass


@app.after_request
def _log_after_response(response):
    try:
        log(f"web: response: {request.method} {request.path} -> {response.status}", "DEBUG")
    except Exception:
        pass
    return response


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    if request.method == "GET":
        cfg = storage.get_config("monitor_config") or {}
        return jsonify(cfg)
    else:
        body = request.get_json(force=True)
        storage.set_config("monitor_config", body)
        return jsonify({"ok": True})


@app.route("/api/classifier", methods=["GET", "POST"])
def api_classifier():
    if request.method == "GET":
        return jsonify(classifier.load_config())
    body = request.get_json(force=True)
    classifier.set_config(body)
    return jsonify({"ok": True})


@app.route("/")
def index():
    log("web: index requested; rendering template index.html", "INFO")
    try:
        return render_template("index.html")
    except Exception as exc:
        log_exc("web: render_template failed for index.html", exc)
        try:
            idx = os.path.join(_TEMPLATES_DIR, "index.html")
            with open(idx, "r", encoding="utf-8") as f:
                content = f.read()
            return Response(content, mimetype="text/html")
        except Exception as exc2:
            log_exc("web: error serving index fallback", exc2)
            return "Not Found", 404


@app.route("/live")
def live():
    log("web: /live requested", "INFO")
    try:
        items = storage.read_items(200)
        log(f"web: /live returning {len(items)} items", "INFO")
        return jsonify(items)
    except Exception as exc:
        log_exc("web: error in /live", exc)
        return jsonify([])


@app.route("/stream")
def stream():
    last_id = storage.get_latest_id()

    def event_stream():
        nonlocal last_id
        while True:
            try:
                items = storage.read_items(200)
                curr = items[0]["id"] if items else 0
                if curr != last_id:
                    last_id = curr
                    yield f"data: {json.dumps(items, ensure_ascii=False)}\n\n"
            except Exception:
                pass
            time.sleep(2)

    return Response(event_stream(), mimetype="text/event-stream")


@app.route("/config", methods=["GET", "POST"])
def config_page():
    if request.method == "GET":
        cfg = storage.get_config("monitor_config") or {}
        classifier_cfg = classifier.load_config() or {}
        sources = ["google", "bing", "reddit", "hn",
                   "newsapi", "x", "facebook", "instagram"]
        return render_template("config.html", sources=sources, params=cfg, classifier=classifier_cfg)

    form = request.form
    cfg = storage.get_config("monitor_config") or {}
    cfg["sources"] = form.getlist("sources") or cfg.get("sources", [])
    try:
        cfg["limit"] = int(form.get("limit", cfg.get("limit", 5)))
    except Exception:
        cfg["limit"] = cfg.get("limit", 5)
    try:
        cfg["interval_minutes"] = int(
            form.get("interval_minutes", cfg.get("interval_minutes", 10)))
    except Exception:
        cfg["interval_minutes"] = cfg.get("interval_minutes", 10)
    cfg["location"] = {
        "state": form.get("location_state", ""),
        "country": form.get("location_country", ""),
        "municipality": form.get("location_municipality", ""),
        "colony": form.get("location_colony", ""),
    }
    cfg["telegram_target_chat"] = form.get("telegram_target_chat", "")
    cfg["telegram_alerts"] = bool(form.get("telegram_alerts"))

    newsapi_opts = cfg.get("source_options", {}).get("newsapi", {})
    domains = (form.get("newsapi_domains", "") or "").strip()
    newsapi_opts["domains"] = [d.strip() for d in domains.split(
        ",") if d.strip()] if domains else newsapi_opts.get("domains", [])
    newsapi_opts["qInTitle"] = bool(form.get("newsapi_qInTitle"))
    newsapi_opts["language"] = form.get("newsapi_language", "")
    newsapi_opts["from_date"] = form.get("newsapi_from_date", "")
    newsapi_opts["to_date"] = form.get("newsapi_to_date", "")
    v = form.get("newsapi_page_size")
    if not v:
        newsapi_opts["page_size"] = newsapi_opts.get("page_size", None)
    else:
        try:
            newsapi_opts["page_size"] = int(v)
        except Exception:
            newsapi_opts["page_size"] = newsapi_opts.get("page_size", None)
    newsapi_opts["sort_by"] = form.get("newsapi_sort_by", "")
    cfg.setdefault("source_options", {})["newsapi"] = newsapi_opts

    classifier_cfg = classifier.load_config() or {}

    def parse_kw_area(v: str):
        if not v:
            return []
        return [s.strip() for s in v.replace('\r', '\n').split('\n') if s.strip()]

    classifier_cfg["high"] = {"keywords": parse_kw_area(
        form.get("classifier_high_keywords", ""))}
    classifier_cfg["medium"] = {"keywords": parse_kw_area(
        form.get("classifier_medium_keywords", ""))}
    classifier_cfg["low"] = {"keywords": parse_kw_area(
        form.get("classifier_low_keywords", ""))}

    cfg["use_context_keywords"] = bool(form.get("use_context_keywords"))
    cfg["context_keywords"] = parse_kw_area(form.get("context_keywords", ""))
    cfg["exclude_keywords"] = parse_kw_area(form.get("exclude_keywords", ""))
    try:
        vloc = form.get("location_radius", "")
        cfg["location_radius"] = int(
            vloc) if vloc else cfg.get("location_radius", None)
    except Exception:
        cfg["location_radius"] = cfg.get("location_radius", None)
    cfg["use_location_filter"] = bool(form.get("use_location_filter"))

    storage.set_config("monitor_config", cfg)
    try:
        classifier.set_config(classifier_cfg)
    except Exception:
        pass

    flash("Configuración guardada", "success")
    return redirect(url_for("config_page"))


@app.route("/api/alerts")
def api_alerts():
    try:
        items = storage.read_items(1000)
        alerts = [it for it in items if it.get("tg_message_id")]
        return jsonify(alerts)
    except Exception as e:
        log_exc("web: error in /api/alerts", e)
        return jsonify([])


@app.route("/alerts")
def alerts_page():
    try:
        cfg = storage.get_config("monitor_config") or {}
        target = cfg.get("telegram_target_chat") if isinstance(
            cfg, dict) else None
        items = storage.read_items(1000)
        alerts = [it for it in items if it.get("tg_message_id")]

        username = None
        if target:
            try:
                t = str(target).strip()
                if t.startswith("http://") or t.startswith("https://"):
                    t = t.rstrip("/").split("/")[-1]
                if t.startswith("t.me/"):
                    t = t.split("/", 1)[1]
                username = t
            except Exception:
                username = None

        return render_template("alerts.html", alerts=alerts, target=target, username=username)
    except Exception as e:
        log_exc("web: error in /alerts", e)
        flash("No se pudieron cargar las alertas.", "danger")
        return redirect(url_for("config_page"))


@app.route("/config/send_test_alert", methods=["POST"])
def config_send_test_alert():
    try:
        cfg = storage.get_config("monitor_config") or {}
        target = None
        try:
            target = cfg.get("telegram_target_chat") if isinstance(
                cfg, dict) else None
        except Exception:
            target = None
        enabled = False
        try:
            enabled = bool(cfg.get("telegram_alerts"))
        except Exception:
            enabled = False

        if not target:
            flash("No hay chat objetivo configurado (telegram_target_chat).", "danger")
            return redirect(url_for("config_page"))
        if not enabled:
            flash(
                "Las alertas Telegram están desactivadas (telegram_alerts=false).", "warning")
            return redirect(url_for("config_page"))

        try:
            core_telegram = importlib.import_module("core.telegram")
        except Exception as e:
            log_exc("web: failed to import core.telegram", e)
            flash("No se pudo cargar el módulo de Telegram. Revisa los logs.", "danger")
            return redirect(url_for("config_page"))

        item = {
            "title": "Prueba: alerta de impacto crítico",
            "url": "https://example.com/test-high-impact",
            "summary": "Mensaje de prueba enviado desde la interfaz de configuración.",
            "published_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            "level": "CRITICO",
            "emoji": "🔴",
            "source": "web_config_test",
        }

        sent = False
        try:
            sent = core_telegram.send_item_notification(item, str(target))
        except Exception as e:
            log_exc("web: exception sending test alert", e)

        if sent:
            flash(f"Mensaje de prueba enviado a {target}", "success")
        else:
            flash(
                f"Fallo al enviar mensaje de prueba a {target}. Revisa logs.", "danger")
        return redirect(url_for("config_page"))
    except Exception as e:
        log_exc("web: unexpected error in send_test_alert", e)
        flash("Error inesperado. Revisa logs.", "danger")
        return redirect(url_for("config_page"))


def run_web(**kwargs):
    app.run(**kwargs)


__all__ = ["app", "run_web"]


if __name__ == "__main__":
    host = os.environ.get("WEB_HOST", "127.0.0.1")
    port = int(os.environ.get("WEB_PORT", "5000"))
    log(f"flask: Iniciando web en http://{host}:{port}/", "INFO")
    try:
        log("flask: URL map:", "DEBUG")
        for r in sorted(str(r) for r in app.url_map.iter_rules()):
            log(f"flask:   {r}", "DEBUG")
    except Exception:
        pass
    run_web(host=host, port=port, debug=False, use_reloader=False)
