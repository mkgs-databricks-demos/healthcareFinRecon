"""Healthcare Revenue Cycle Reconciliation — SDP Pipeline entry point.

Metadata-driven pipeline using YAML-injected table definitions, schemas,
and expectations. No DLT imports; no legacy prefix syntax; no dp.read/dp.read_stream.

This file is loaded as the single library in the DABs pipeline YAML.
All SDP tables and views are dynamically registered into this module's
globals() so SDP can discover them.
"""
from __future__ import annotations

import os
import sys

# ── Path setup: make `base` importable ────────────────────────────────────────
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# ── Runtime config (set by pipeline YAML configuration block) ────────────────
catalog     = spark.conf.get("pipeline.catalog_use")
schema      = spark.conf.get("pipeline.schema_use")
bundle_path = spark.conf.get("pipeline.bundle_files_path")
config_dir  = os.path.join(bundle_path, "src", "pipeline", "config")

# ── Factory imports ───────────────────────────────────────────────────────────
from base.bronze_factory import BronzeFactory
from base.silver_factory import SilverFactory
from base.gold_factory   import GoldFactory


# ════════════════════════════════════════════════════════════════════════════════
# SILVER TRANSFORM FUNCTIONS
# Each returns a streaming DataFrame from the corresponding bronze table.
# The DataFrame is registered as a @dp.temporary_view by SilverFactory.
# ════════════════════════════════════════════════════════════════════════════════

def _silver_rc_claim_src(spark: SparkSession, catalog: str, schema: str):
    """Source view for silver_rc_claim CDC flow.
    Reads bronze_rc_claims, filters nulls, casts all columns.
    """
    return (
        spark.readStream.table(f"{catalog}.{schema}.bronze_rc_claims")
        .where("claim_id IS NOT NULL")
        .select(
            F.col("claim_id").cast("string"),
            F.col("patient_id").cast("string"),
            F.col("provider_npi").cast("string"),
            F.col("provider_organization").cast("string"),
            F.col("payer_id").cast("string"),
            F.col("payer_name").cast("string"),
            F.col("claim_type").cast("string"),
            F.to_date(F.col("service_date")).alias("service_date"),
            F.to_date(F.col("submission_date")).alias("submission_date"),
            F.col("billed_amount").cast("decimal(18,2)"),
            F.col("procedure_code").cast("string"),
            F.col("diagnosis_code").cast("string"),
            F.col("place_of_service").cast("string"),
            F.col("_ingest_ts"),
            F.col("_source_ts"),
            F.col("_source_file"),
        )
    )


def _silver_rc_eob_src(spark: SparkSession, catalog: str, schema: str):
    """Source view for silver_rc_eob CDC flow.
    Reads bronze_rc_eob, filters nulls, casts all columns.
    """
    return (
        spark.readStream.table(f"{catalog}.{schema}.bronze_rc_eob")
        .where("claim_id IS NOT NULL AND eob_id IS NOT NULL")
        .select(
            F.col("eob_id").cast("string"),
            F.col("claim_id").cast("string"),
            F.col("payer_id").cast("string"),
            F.to_date(F.col("payment_date")).alias("payment_date"),
            F.col("paid_amount").cast("decimal(18,2)"),
            F.col("denied_amount").cast("decimal(18,2)"),
            F.col("adjusted_amount").cast("decimal(18,2)"),
            F.col("adjudication_status").cast("string"),
            F.col("denial_reason_code").cast("string"),
            F.col("denial_reason_desc").cast("string"),
            F.col("_ingest_ts"),
            F.col("_source_ts"),
            F.col("_source_file"),
        )
    )


# ════════════════════════════════════════════════════════════════════════════════
# GOLD TRANSFORM FUNCTIONS
# Each returns a batch DataFrame (not streaming) for @dp.materialized_view.
# ════════════════════════════════════════════════════════════════════════════════

def transform_gold_rc_claim(spark: SparkSession, catalog: str, schema: str):
    """Gold claims: silver_rc_claim enriched with healthcare fiscal year/period tags.
    Fiscal year starts July 1. FY2025 = July 2024 – June 2025.
    """
    return spark.sql(f"""
        SELECT
            c.*,
            CASE WHEN MONTH(c.service_date) >= 7
                 THEN YEAR(c.service_date) + 1
                 ELSE YEAR(c.service_date)
            END AS fiscal_year,
            CASE WHEN MONTH(c.service_date) >= 7
                 THEN MONTH(c.service_date) - 6
                 ELSE MONTH(c.service_date) + 6
            END AS fiscal_period
        FROM {catalog}.{schema}.silver_rc_claim c
    """)


def transform_gold_rc_eob(spark: SparkSession, catalog: str, schema: str):
    """Gold EOB: silver_rc_eob joined to gold_rc_claim for context + variance."""
    return spark.sql(f"""
        SELECT
            e.*,
            c.billed_amount,
            c.provider_npi,
            c.provider_organization,
            c.payer_name,
            c.claim_type,
            c.service_date,
            c.fiscal_year,
            c.fiscal_period,
            c.billed_amount
                - COALESCE(e.paid_amount,   0.00)
                - COALESCE(e.denied_amount, 0.00)  AS variance_amount
        FROM {catalog}.{schema}.silver_rc_eob e
        INNER JOIN {catalog}.{schema}.gold_rc_claim c
            ON e.claim_id = c.claim_id
    """)


def transform_gold_rc_recon_exception(spark: SparkSession, catalog: str, schema: str):
    """Gold reconciliation exceptions: claims with open, partial, or unexplained variances.
    Includes AR aging buckets for accounts receivable management.
    """
    return spark.sql(f"""
        SELECT
            c.claim_id,
            c.patient_id,
            c.provider_npi,
            c.provider_organization,
            c.payer_id,
            c.payer_name,
            c.claim_type,
            c.service_date,
            c.submission_date,
            c.fiscal_year,
            c.fiscal_period,
            c.billed_amount,
            COALESCE(e.paid_amount,   0.00)                              AS paid_amount,
            COALESCE(e.denied_amount, 0.00)                              AS denied_amount,
            c.billed_amount
                - COALESCE(e.paid_amount,   0.00)
                - COALESCE(e.denied_amount, 0.00)                        AS variance_amount,
            COALESCE(e.adjudication_status, 'unsubmitted')               AS adjudication_status,
            e.denial_reason_code,
            e.denial_reason_desc,
            DATEDIFF(current_date(), c.service_date)                     AS days_outstanding,
            CASE
                WHEN DATEDIFF(current_date(), c.service_date) <=  30 THEN '0-30'
                WHEN DATEDIFF(current_date(), c.service_date) <=  60 THEN '31-60'
                WHEN DATEDIFF(current_date(), c.service_date) <=  90 THEN '61-90'
                WHEN DATEDIFF(current_date(), c.service_date) <= 120 THEN '91-120'
                ELSE '120+'
            END                                                          AS ar_aging_bucket,
            current_timestamp()                                          AS _gold_ts
        FROM {catalog}.{schema}.gold_rc_claim c
        LEFT JOIN {catalog}.{schema}.silver_rc_eob e
            ON c.claim_id = e.claim_id
        WHERE
            e.claim_id IS NULL
            OR e.adjudication_status IN ('pending', 'partial', 'denied')
            OR ABS(
                c.billed_amount
                - COALESCE(e.paid_amount,   0.00)
                - COALESCE(e.denied_amount, 0.00)
            ) > 0.01
    """)


def transform_gold_rc_fiscal_calendar(spark: SparkSession, catalog: str, schema: str):
    """Gold fiscal calendar dimension.
    Span: 2020-07-01 through 2030-06-30.
    FY starts July 1: July=P1, August=P2, ..., June=P12.
    FY2025 = July 2024 through June 2025.
    """
    return spark.sql("""
        WITH date_spine AS (
            SELECT EXPLODE(SEQUENCE(
                DATE '2020-07-01',
                DATE '2030-06-30',
                INTERVAL 1 DAY
            )) AS calendar_date
        )
        SELECT
            calendar_date,
            YEAR(calendar_date)                                                AS calendar_year,
            MONTH(calendar_date)                                               AS calendar_month,
            CASE WHEN MONTH(calendar_date) >= 7
                 THEN YEAR(calendar_date) + 1
                 ELSE YEAR(calendar_date)
            END                                                                AS fiscal_year,
            CASE WHEN MONTH(calendar_date) >= 7
                 THEN MONTH(calendar_date) - 6
                 ELSE MONTH(calendar_date) + 6
            END                                                                AS fiscal_period,
            CONCAT('P', LPAD(CAST(
                CASE WHEN MONTH(calendar_date) >= 7
                     THEN MONTH(calendar_date) - 6
                     ELSE MONTH(calendar_date) + 6
                END AS STRING), 2, '0'))                                       AS fiscal_period_label,
            CEIL(CASE WHEN MONTH(calendar_date) >= 7
                      THEN MONTH(calendar_date) - 6
                      ELSE MONTH(calendar_date) + 6
                 END / 3.0)                                                    AS fiscal_quarter,
            DATE_FORMAT(calendar_date, 'MMMM yyyy')                            AS month_name,
            DAYOFWEEK(calendar_date)                                           AS day_of_week,
            CASE WHEN DAYOFWEEK(calendar_date) IN (1, 7)
                 THEN true ELSE false
            END                                                                AS is_weekend
        FROM date_spine
    """)


# ════════════════════════════════════════════════════════════════════════════════
# FACTORY REGISTRATION
# Factories read YAML configs and register SDP tables/views into globals().
# Order matters: Bronze → Silver → Gold (dependency order).
# ════════════════════════════════════════════════════════════════════════════════

# ── Bronze ────────────────────────────────────────────────────────────────────
bronze_factory = BronzeFactory(
    config_dir=os.path.join(config_dir, "bronze"),
    spark=spark,
    catalog=catalog,
    schema=schema,
)
bronze_factory.register_all(target_globals=globals())

# ── Silver ────────────────────────────────────────────────────────────────────
silver_factory = SilverFactory(
    config_dir=os.path.join(config_dir, "silver"),
    spark=spark,
    catalog=catalog,
    schema=schema,
    src_transforms={
        "silver_rc_claim": _silver_rc_claim_src,
        "silver_rc_eob":   _silver_rc_eob_src,
    },
)
silver_factory.register_all(target_globals=globals())

# ── Gold ──────────────────────────────────────────────────────────────────────
gold_factory = GoldFactory(
    config_dir=os.path.join(config_dir, "gold"),
    spark=spark,
    catalog=catalog,
    schema=schema,
    transforms={
        "gold_rc_claim":              transform_gold_rc_claim,
        "gold_rc_eob":                transform_gold_rc_eob,
        "gold_rc_recon_exception":    transform_gold_rc_recon_exception,
        "gold_rc_fiscal_calendar":    transform_gold_rc_fiscal_calendar,
    },
)
gold_factory.register_all(target_globals=globals())
