from __future__ import annotations

import logging
from typing import Optional

_SUMMARIZER = None
_SUM_TOKENIZER = None
_TOPIC_MODEL = None
_TOPIC_TOKENIZER = None


def _ensure_transformers():
    try:
        import transformers
        return True
    except Exception:
        logging.warning(
            "transformers no está instalado; instala 'transformers' y 'torch' para usar modelos")
        return False


def load_summarizer(model_name: str = "google/pegasus-xsum"):
    global _SUMMARIZER, _SUM_TOKENIZER
    if _SUMMARIZER is not None:
        return
    if not _ensure_transformers():
        return
    try:
        from transformers import PegasusForConditionalGeneration, AutoTokenizer
        _SUM_TOKENIZER = AutoTokenizer.from_pretrained(model_name)
        _SUMMARIZER = PegasusForConditionalGeneration.from_pretrained(
            model_name)
        logging.info(f"Summarizer loaded: {model_name}")
    except Exception as e:
        logging.exception(f"Error loading summarizer {model_name}: {e}")


def load_topic_model(model_name: str = "mrm8488/t5-base-finetuned-news-title-classification"):
    global _TOPIC_MODEL, _TOPIC_TOKENIZER
    if _TOPIC_MODEL is not None:
        return
    if not _ensure_transformers():
        return
    try:
        from transformers import T5ForConditionalGeneration, AutoTokenizer
        _TOPIC_TOKENIZER = AutoTokenizer.from_pretrained(model_name)
        _TOPIC_MODEL = T5ForConditionalGeneration.from_pretrained(model_name)
        logging.info(f"Topic model loaded: {model_name}")
    except Exception as e:
        logging.exception(f"Error loading topic model {model_name}: {e}")


def summarize_text(text: str, model_name: str = "google/pegasus-xsum") -> str:
    if not text:
        return ""
    load_summarizer(model_name)
    if _SUMMARIZER is None or _SUM_TOKENIZER is None:
        return ""
    try:
        inputs = _SUM_TOKENIZER([text], truncation=True,
                                padding="longest", return_tensors="pt")
        summary_ids = _SUMMARIZER.generate(
            **inputs, max_length=150, num_beams=4)
        summary = _SUM_TOKENIZER.decode(
            summary_ids[0], skip_special_tokens=True)
        return summary
    except Exception:
        logging.exception("Error during summarization")
        return ""


def classify_topic(text: str, model_name: str = "mrm8488/t5-base-finetuned-news-title-classification") -> str:
    if not text:
        return "general"
    load_topic_model(model_name)
    if _TOPIC_MODEL is None or _TOPIC_TOKENIZER is None:
        return "general"
    try:
        inputs = _TOPIC_TOKENIZER.encode(
            text, return_tensors="pt", truncation=True, max_length=512)
        outputs = _TOPIC_MODEL.generate(inputs, max_new_tokens=50)
        topic = _TOPIC_TOKENIZER.decode(outputs[0], skip_special_tokens=True)
        return topic.lower().strip()
    except Exception:
        logging.exception("Error during topic classification")
        return "general"
