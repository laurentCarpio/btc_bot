from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, List
import json
import tempfile

import boto3
import xgboost as xgb


@dataclass(slots=True)
class LoadedModelArtifact:
    model: Any
    feature_cols: List[str]
    threshold: float


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


def _download_s3_file(s3_path: str, local_path: str) -> None:
    bucket, key = _split_s3_path(s3_path)
    s3 = boto3.client("s3")
    s3.download_file(bucket, key, local_path)


class XGBoostProbModel:
    def __init__(self, booster: xgb.Booster, feature_cols: list[str]) -> None:
        self.booster = booster
        self.feature_cols = feature_cols

    def predict_proba(self, X):
        dmat = xgb.DMatrix(X[self.feature_cols], feature_names=self.feature_cols)
        p = self.booster.predict(dmat)
        return [[1.0 - float(v), float(v)] for v in p]


def load_xgb_artifact_from_run(
    run_root: str,
    threshold: float = 0.70,
) -> LoadedModelArtifact:
    run_root = run_root.rstrip("/")

    run_config_path = f"{run_root}/run_config.json"
    cfg = _read_json_s3(run_config_path)

    feature_cols = cfg.get("feature_cols")
    if not feature_cols:
        raise ValueError(f"No feature_cols found in {run_config_path}")

    model_name = cfg.get("model_artifact", "xgb_model.json")
    model_s3_path = f"{run_root}/{model_name}"

    with tempfile.TemporaryDirectory() as td:
        local_model = str(Path(td) / model_name)
        _download_s3_file(model_s3_path, local_model)

        booster = xgb.Booster()
        booster.load_model(local_model)

    wrapped = XGBoostProbModel(booster=booster, feature_cols=feature_cols)

    return LoadedModelArtifact(
        model=wrapped,
        feature_cols=feature_cols,
        threshold=float(threshold),
    )