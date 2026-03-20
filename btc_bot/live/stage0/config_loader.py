from __future__ import annotations

import json
import boto3

from dataclasses import fields
from btc_bot.live.config import Stage0Config, ThresholdConfig


def _split_s3_path(s3_path: str) -> tuple[str, str]:
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Expected s3 path, got: {s3_path}")
    no_scheme = s3_path[len("s3://"):]
    bucket, key = no_scheme.split("/", 1)
    return bucket, key

def _read_json_s3(s3_path: str) -> dict:
    bucket, key = _split_s3_path(s3_path)
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

def _filter_payload_for_dataclass(payload: dict, cls):
    allowed = {f.name for f in fields(cls)}
    return {k: v for k, v in payload.items() if k in allowed}

def load_thresholds_from_s3(s3_path: str) -> ThresholdConfig:
    payload = _read_json_s3(s3_path)
    payload = _filter_payload_for_dataclass(payload, ThresholdConfig)
    return ThresholdConfig(**payload)

def load_stage0_config_from_s3(s3_path: str) -> Stage0Config:
    payload = _read_json_s3(s3_path)
    payload = _filter_payload_for_dataclass(payload, Stage0Config)

    # JSON list -> tuple for depths
    if "depths" in payload and isinstance(payload["depths"], list):
        payload["depths"] = tuple(int(x) for x in payload["depths"])

    return Stage0Config(**payload)

def load_vol_bucket_edges_from_s3(s3_path: str) -> list[float]:
    payload = _read_json_s3(s3_path)
    edges = payload.get("edges")
    if not isinstance(edges, list) or not edges:
        raise ValueError(f"Missing or invalid 'edges' in {s3_path}")
    return [float(x) for x in edges]