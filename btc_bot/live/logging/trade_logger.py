from __future__ import annotations

import logging
from typing import Optional
import json 
import colorlog
import watchtower

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_GROUP_NAME = "TradebotLogs"


def _has_handler_type(logger: logging.Logger, handler_type: type) -> bool:
    return any(isinstance(h, handler_type) for h in logger.handlers)

def log_candidate_event(logger: logging.Logger, payload: dict) -> None:
    logger.info(json.dumps(payload, ensure_ascii=False, sort_keys=True))

def setup_logger(
    name: str,
    *,
    logger_level: int = logging.INFO,
    console_level: int = logging.INFO,
    cw_level: int = logging.WARNING,
    log_group: str = LOG_GROUP_NAME,
    stream_name: Optional[str] = None,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logger_level)
    logger.propagate = False

    if stream_name is None:
        stream_name = name

    # Console handler
    if not _has_handler_type(logger, logging.StreamHandler):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)

        console_formatter = colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
            datefmt=TIMESTAMP_FORMAT,
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    # CloudWatch handler
    if not _has_handler_type(logger, watchtower.CloudWatchLogHandler):
        cw_handler = watchtower.CloudWatchLogHandler(
            log_group=log_group,
            stream_name=stream_name,
        )
        cw_handler.setLevel(cw_level)

        cw_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
            datefmt=TIMESTAMP_FORMAT,
        )
        cw_handler.setFormatter(cw_formatter)
        logger.addHandler(cw_handler)

    return logger


# Logger principal du pipeline live
logger_pub = setup_logger(
    "logger_pub",
    logger_level=logging.INFO,
    console_level=logging.INFO,
    cw_level=logging.WARNING,
)

# Logger dédié aux candidats / accept / événements utiles à exploiter
logger_cand = setup_logger(
    "logger_cand",
    logger_level=logging.INFO,
    console_level=logging.INFO,
    cw_level=logging.INFO,
)