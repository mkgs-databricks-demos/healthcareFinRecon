"""Base class and shared utilities for SDP table/view factories."""
from __future__ import annotations

import yaml
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from pyspark import pipelines as dp
from pyspark.sql import SparkSession


def load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def normalize_cluster_by(v: Any) -> list[str] | None:
    if v is None:
        return None
    if isinstance(v, str):
        return [v]
    return list(v)


def expectation_decorator(mode: str, rules: dict[str, str]):
    """Return dp.expect_all_* for the given mode string."""
    mapping = {
        "record": dp.expect_all,
        "warn":   dp.expect_all,
        "drop":   dp.expect_all_or_drop,
        "fail":   dp.expect_all_or_fail,
    }
    deco_fn = mapping.get(mode, dp.expect_all)
    return deco_fn(rules)


DEFAULT_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed":       "true",
    "delta.enableDeletionVectors":      "true",
    "delta.enableRowTracking":          "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact":   "true",
}


class SDPTableFactory(ABC):
    """Abstract factory for SDP table/view creation driven by YAML config."""

    def __init__(self, config_dir: str | Path, spark: SparkSession, catalog: str, schema: str):
        self.config_dir = Path(config_dir)
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    @abstractmethod
    def register_from_yaml(self, yaml_path: str | Path, target_globals: dict) -> None:
        """Load YAML and register SDP tables/views into target_globals."""
        ...

    def register_all(self, target_globals: dict, glob: str = "*.yml") -> None:
        """Discover all YAML files in config_dir and register each one."""
        for yaml_file in sorted(self.config_dir.glob(glob)):
            self.register_from_yaml(yaml_file, target_globals)

    def _merge_table_properties(self, overrides: dict | None) -> dict[str, str]:
        merged = {**DEFAULT_TABLE_PROPERTIES}
        if overrides:
            merged.update({k: str(v) for k, v in overrides.items()})
        return merged
