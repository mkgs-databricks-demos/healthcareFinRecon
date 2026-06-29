"""SilverFactory: CDC streaming tables (temp_view → streaming_table → cdc_flow) from YAML."""
from __future__ import annotations

from pathlib import Path
from typing import Callable

import pyspark.sql.functions as F
from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from base.factory import SDPTableFactory, load_yaml, normalize_cluster_by, expectation_decorator


class SilverFactory(SDPTableFactory):
    """Registers silver CDC tables from YAML using create_streaming_table + create_auto_cdc_flow."""

    def __init__(
        self,
        config_dir: str | Path,
        spark: SparkSession,
        catalog: str,
        schema: str,
        src_transforms: dict[str, Callable],
    ):
        super().__init__(config_dir, spark, catalog, schema)
        self.src_transforms = src_transforms

    def register_from_yaml(self, yaml_path: str | Path, target_globals: dict) -> None:
        cfg = load_yaml(yaml_path)
        name = cfg["name"]

        if name not in self.src_transforms:
            raise ValueError(
                f"SilverFactory: no src_transform registered for '{name}'. "
                f"Available: {list(self.src_transforms.keys())}"
            )

        comment = cfg.get("comment", "")
        keys = list(cfg["keys"])
        sequence_by = cfg["sequence_by"]
        schema_ddl = cfg.get("schema")
        cluster_by = normalize_cluster_by(cfg.get("cluster_by"))
        table_properties = self._merge_table_properties(cfg.get("table_properties"))

        src_view_name = f"{name}_src"
        transform_fn = self.src_transforms[name]
        spark = self.spark
        catalog = self.catalog
        schema_name = self.schema

        # 1. Temporary view
        exp_cfg = cfg.get('expectations')

        def _src_view():
            return transform_fn(spark, catalog, schema_name)

        _src_view.__name__ = src_view_name
        _src_view.__qualname__ = src_view_name

        if exp_cfg:
            mode = exp_cfg.get('mode', 'record')
            rules = exp_cfg.get('rules', {})
            _src_view = expectation_decorator(mode, rules)(_src_view)

        target_globals[src_view_name] = dp.temporary_view(name=src_view_name)(_src_view)

        # 2. CDC target table
        dp.create_streaming_table(
            name=name,
            comment=comment,
            schema=schema_ddl,
            cluster_by=cluster_by,
            table_properties=table_properties,
        )

        # 3. CDC flow (SCD Type 1 — latest wins)
        dp.create_auto_cdc_flow(
            target=name,
            source=src_view_name,
            keys=keys,
            sequence_by=F.col(sequence_by),
            stored_as_scd_type=1,
        )
