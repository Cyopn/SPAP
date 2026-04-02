from __future__ import annotations

import re
import unicodedata
from typing import List, Union, Tuple, Dict, Any
from core import storage


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
    txt = _normalize_for_match(text)
    return [t for t in txt.split() if t]


def _normalize_for_match(text: str) -> str:
    if not text:
        return ""
    txt = str(text).lower()
    txt = "".join(
        ch for ch in unicodedata.normalize("NFD", txt)
        if unicodedata.category(ch) != "Mn"
    )
    txt = re.sub(r"[^a-z0-9]+", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _prepare_keywords(raw_keywords: List[str]) -> List[Tuple[str, str]]:
    prepared: List[Tuple[str, str]] = []
    seen: set[str] = set()
    for raw in raw_keywords or []:
        val = str(raw or "").strip()
        if not val:
            continue
        norm = _normalize_for_match(val)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        prepared.append((norm, val))
    return prepared


def _find_matches(normalized_text: str, prepared: List[Tuple[str, str]]) -> List[str]:
    if not normalized_text or not prepared:
        return []
    text_pad = f" {normalized_text} "
    found: List[str] = []
    for norm_kw, raw_kw in prepared:
        needle = f" {norm_kw} "
        if needle in text_pad:
            found.append(raw_kw)
    return found


def classify_text(text: str, title: str | None = None, keyword: str | None = None) -> Union[Dict[str, Any], Tuple[str, str, str], str]:
    cfg = load_config()

    alto_prepared = _prepare_keywords(
        (cfg.get("alto") or {}).get("keywords", []))
    med_prepared = _prepare_keywords(
        (cfg.get("medio") or {}).get("keywords", []))
    bajo_prepared = _prepare_keywords(
        (cfg.get("bajo") or {}).get("keywords", []))

    combined = " ".join(filter(None, [title or "", text or "", keyword or ""]))
    normalized = _normalize_for_match(combined)
    tokens = _tokenize(combined)

    matched_alto = _find_matches(normalized, alto_prepared)
    matched_medio = _find_matches(normalized, med_prepared)
    matched_bajo = _find_matches(normalized, bajo_prepared)

    neg_words_norm = {_normalize_for_match(w) for w in NEGATIVE_WORDS}
    viral_words_norm = {_normalize_for_match(w) for w in VIRAL_WORDS}

    neg_count = sum(1 for t in tokens if t in neg_words_norm)
    viral_score = sum(1 for t in tokens if t in viral_words_norm)
    sentimiento = "negativo" if neg_count > 0 else "neutral"

    if matched_alto:
        impacto = "alto"
        base_score = 8
    elif matched_medio:
        impacto = "medio"
        base_score = 5
    elif matched_bajo:
        impacto = "bajo"
        base_score = 2
    else:
        impacto = "bajo"
        base_score = 1

    nivel_riesgo = max(
        1, min(10, base_score + min(2, neg_count) + min(2, viral_score)))

    detected: List[str] = []
    for kw in matched_alto + matched_medio + matched_bajo:
        if kw not in detected:
            detected.append(kw)

    just_parts: List[str] = []
    if matched_alto:
        just_parts.append(
            f"Coincidencia alto: {', '.join(sorted(matched_alto))}")
    elif matched_medio:
        just_parts.append(
            f"Coincidencia medio: {', '.join(sorted(matched_medio))}")
    elif matched_bajo:
        just_parts.append(
            f"Coincidencia bajo: {', '.join(sorted(matched_bajo))}")
    else:
        just_parts.append("Sin coincidencias de keywords por nivel")

    if sentimiento == "negativo":
        just_parts.append("Tono emocional negativo detectado")
    if viral_score:
        just_parts.append("Indicadores de viralización")

    justificacion = "; ".join(just_parts)

    return {
        "impacto": impacto,
        "nivel_riesgo": nivel_riesgo,
        "sentimiento": sentimiento,
        "palabras_clave_detectadas": sorted(detected),
        "justificacion": justificacion,
    }


DEFAULT_CONFIG = {
    "alto": {"keywords": [
        "ataque", "asesinato", "homicidio", "balacera", "explosión", "explosion",
        "incendio", "secuestro", "desaparecido", "crisis", "emergencia", "urgente",
        "denuncia", "corrupción", "escándalo", "fraude", "robo masivo", "protesta",
        "bloqueo", "disturbios", "colapso", "accidente grave", "muerte", "muerto", "muertos",
        "fallecido", "fallecidos", "feminicidio"
    ]},
    "medio": {"keywords": [
        "crítica", "critica", "queja", "denuncia leve", "problema", "fallas", "inconformidad",
        "retraso", "afectación", "afectacion", "investigación", "posible", "preocupación", "alerta"
    ]},
    "bajo": {"keywords": [
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
            "alto": {"keywords": cfg.get("alto", {}).get("keywords", [])},
            "medio": {"keywords": cfg.get("medio", {}).get("keywords", [])},
            "bajo": {"keywords": cfg.get("bajo", {}).get("keywords", [])},
        }
        return out
    except Exception:
        return DEFAULT_CONFIG.copy()


def set_config(cfg: Dict[str, Any]) -> None:
    try:
        storage.set_config("classifier_config", cfg)
    except Exception:
        pass
