from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Union, Tuple, Dict, Any
from core import storage


CRITICAL_KEYWORDS = {
    "balacera",
    "asesinato",
    "explosión",
    "explosion",
    "incendio",
    "secuestro",
    "desaparecido",
    "accidente",
    "accidente grave",
    "corrupción",
    "escándalo",
    "fraude",
    "denuncia",
    "colapso",
    "crisis",
    "ataque",
    "homicidio",
    "robo masivo",
    "manifestación",
    "disturbio",
    "bloqueo",
}

MEDIUM_KEYWORDS = {
    "falla",
    "fallas",
    "queja",
    "quejas",
    "retraso",
    "afectación",
    "afectaciones",
    "molestia",
    "incomodidad",
    "preocupación",
    "crítica",
    "critica",
    "posible",
    "investigación",
    "investigacion",
    "alerta",
    "reportan",
}

LOW_KEYWORDS = {
    "informó",
    "informo",
    "anunció",
    "anuncio",
    "reportó",
    "reporto",
    "evento",
    "inauguración",
    "inauguracion",
    "programa",
    "participación",
    "participacion",
    "actividad",
    "datos",
    "estadísticas",
    "estadisticas",
}

NEGATIVE_WORDS = {
    "indignación",
    "indignacion",
    "enojo",
    "enoj",
    "repudio",
    "odio",
    "miedo",
}

VIRAL_WORDS = {"viral", "tendencia", "tendencias",
               "miles", "viralizar", "viralizado"}


def _tokenize(text: str) -> List[str]:
    if not text:
        return []
    txt = text.lower()
    for ch in ".,;:()[]{}\n\t\r\"'":
        txt = txt.replace(ch, " ")
    return [t for t in txt.split() if t]


def classify_text(text: str, title: str | None = None, keyword: str | None = None) -> Union[Dict[str, Any], Tuple[str, str, str], str]:
    combined = " ".join(filter(None, [title or "", text or "", keyword or ""]))
    tokens = _tokenize(combined)

    detected = set()
    for t in tokens:
        if t in CRITICAL_KEYWORDS:
            detected.add(t)
        elif t in MEDIUM_KEYWORDS:
            detected.add(t)
        elif t in LOW_KEYWORDS:
            detected.add(t)
        elif t in NEGATIVE_WORDS:
            detected.add(t)
        elif t in VIRAL_WORDS:
            detected.add(t)

    neg_count = sum(1 for t in tokens if t in NEGATIVE_WORDS)
    if neg_count > 0:
        sentimiento = "negativo"
    else:
        sentimiento = "neutral"

    viral_score = sum(1 for t in tokens if t in VIRAL_WORDS)

    score = 0
    if any(t in CRITICAL_KEYWORDS for t in tokens):
        impacto = "CRITICO"
        score += 8
    elif any(t in MEDIUM_KEYWORDS for t in tokens):
        impacto = "MEDIO"
        score += 4
    elif any(t in LOW_KEYWORDS for t in tokens):
        impacto = "BAJO"
        score += 1
    else:
        impacto = "BAJO"

    score += min(3, neg_count)
    score += min(3, viral_score * 2)

    nivel_riesgo = max(1, min(10, score))
    
    critical_trigger = any(k in tokens for k in [
                           "balacera", "asesinato", "explosión", "explosion", "incendio", "secuestro", "desaparecido", "accidente"])
    title_tokens = _tokenize(title or "")
    combined_len = len(combined)
    if critical_trigger:
        if any(k in title_tokens for k in ["balacera", "asesinato", "explosión", "explosion", "incendio", "secuestro", "desaparecido", "accidente"]) or combined_len > 100 or neg_count > 0:
            impacto = "CRITICO"
            nivel_riesgo = max(nivel_riesgo, 9)
        else:
            impacto = "MEDIO"
            nivel_riesgo = max(nivel_riesgo, 5)

    if not detected:
        impacto = "MEDIO"

    just_parts = []
    if detected:
        just_parts.append(f"Palabras clave: {', '.join(sorted(detected))}")
    if sentimiento == "negativo":
        just_parts.append("Tono emocional negativo detectado")
    if viral_score:
        just_parts.append("Indicadores de viralización")
    if not just_parts:
        just_parts.append("Contenido ambiguo o informativo")

    justificacion = "; ".join(just_parts)

    return {
        "impacto": impacto,
        "nivel_riesgo": nivel_riesgo,
        "sentimiento": sentimiento,
        "palabras_clave_detectadas": sorted(detected),
        "justificacion": justificacion,
    }


DEFAULT_CONFIG = {
    "high": {"keywords": [
        "ataque", "asesinato", "homicidio", "balacera", "explosión", "explosion",
        "incendio", "secuestro", "desaparecido", "crisis", "emergencia", "urgente",
        "denuncia", "corrupción", "escándalo", "fraude", "robo masivo", "protesta",
        "bloqueo", "disturbios", "colapso", "accidente grave"
    ]},
    "medium": {"keywords": [
        "crítica", "critica", "queja", "denuncia leve", "problema", "fallas", "inconformidad",
        "retraso", "afectación", "afectacion", "investigación", "posible", "preocupación", "alerta"
    ]},
    "low": {"keywords": [
        "informó", "informo", "anunció", "anuncio", "reportó", "reporto", "evento", "inauguración",
        "programa", "participación", "participacion", "actividad", "datos", "estadísticas", "estadisticas"
    ]},
}


def load_config() -> Dict[str, Any]:
    try:
        cfg = storage.get_config("classifier_config")
        if not cfg:
            return DEFAULT_CONFIG.copy()
        out = {
            "high": {"keywords": cfg.get("high", {}).get("keywords", [])},
            "medium": {"keywords": cfg.get("medium", {}).get("keywords", [])},
            "low": {"keywords": cfg.get("low", {}).get("keywords", [])},
        }
        return out
    except Exception:
        return DEFAULT_CONFIG.copy()


def set_config(cfg: Dict[str, Any]) -> None:
    try:
        storage.set_config("classifier_config", cfg)
    except Exception:
        pass
