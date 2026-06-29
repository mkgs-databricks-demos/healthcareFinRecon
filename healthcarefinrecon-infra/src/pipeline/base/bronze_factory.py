"""BronzeFactory: Auto Loader cloudFiles streaming tables driven by YAML config."""
from __future__ import annotations

from pathlib import Path

import pyspark.sql.functions as F
from pyspark import pipelines as dp

from base.factory import SDPTableFactory, load_yaml, normalize_cluster_by


class BronzeFactory(SDPTableFactory):
    """Registers bronze streaming tables from YAML using Auto Loader (cloudFiles)."""

    def register_from_yaml(self, yaml_path: str | Path, target_globals: dict) -> None:
        cfg = load_yaml(yaml_path)
        name = cfg["name"]
        comment = cfg.get("comment", "")
        src = cfg["source"]

        landing_path = src["landing_path"].format(
            catalog=self.catalog,
            schema=self.schema,
        )

        cloud_files_opts = {
            "cloudFiles.format":               src["format"],
            "cloudFiles.inferColumnTypes":     str(src.get("infer_column_types", True)).lower(),
            "cloudFiles.includeExistingFiles": str(src.get("include_existing_files", True)).lower(),
            "cloudFiles.schemaLocation":       (
                f"/Volumes/{self.catalog}/{self.schema}/bronze_landing/_schema/{name}"
            ),
        }

        cluster_by = normalize_cluster_by(cfg.get("cluster_by"))
        table_properties = self._merge_table_properties(cfg.get("table_properties"))

        spark = self.spark

        def _read_fn():
            return (
                spark.readStream
                .format("cloudFiles")
                .options(**cloud_files_opts)
                .load(landing_path)
                .withColumn("_source_file", F.col("_metadata.file_path"))
                .withColumn("_source_ts",   F.col("_metadata.file_modification_time"))
                .withColumn("_ingest_ts",   F.current_timestamp())
            )

        _read_fn.__name__ = name
        _read_fn.__qualname__ = name

        decorated = dp.table(
            name=name,
            comment=comment,
            cluster_by=cluster_by,
            table_properties=table_properties,
        )(_read_fn)

        target_globals[name] = decorated
