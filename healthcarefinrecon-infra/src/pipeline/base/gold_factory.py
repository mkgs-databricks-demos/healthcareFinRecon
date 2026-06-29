"""GoldFactory: Materialized views driven by YAML config."""
from __future__ import annotations

from pathlib import Path
from typing import Callable

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from base.factory import SDPTableFactory, load_yaml, normalize_cluster_by


class GoldFactory(SDPTableFactory):
    """Registers gold materialized views from YAML using @dp.materialized_view."""

    def __init__(
        self,
        config_dir: str | Path,
        spark: SparkSession,
        catalog: str,
        schema: str,
        transforms: dict[str, Callable],
    ):
        super().__init__(config_dir, spark, catalog, schema)
        self.transforms = transforms

    def register_from_yaml(self, yaml_path: str | Path, target_globals: dict) -> None:
        cfg = load_yaml(yaml_path)
        name = cfg["name"]

        if name not in self.transforms:
            raise ValueError(
                f"GoldFactory: no transform registered for '{name}'. "
                f"Available: {list(self.transforms.keys())}"
            )

        comment = cfg.get("comment", "")
        cluster_by = normalize_cluster_by(cfg.get("cluster_by"))
        cluster_by_auto = cfg.get("cluster_by_auto", False)
        table_properties = self._merge_table_properties(cfg.get("table_properties"))

        transform_fn = self.transforms[name]
        spark = self.spark
        catalog = self.catalog
        schema_name = self.schema

        def _mv_fn():
            return transform_fn(spark, catalog, schema_name)

        _mv_fn.__name__ = name
        _mv_fn.__qualname__ = name

        decorated = dp.materialized_view(
            name=name,
            comment=comment,
            cluster_by=cluster_by,
            cluster_by_auto=cluster_by_auto,
            table_properties=table_properties,
        )(_mv_fn)

        target_globals[name] = decorated
