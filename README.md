# healthcareFinRecon

Healthcare Revenue Cycle Reconciliation on Databricks — two-bundle DABs architecture.

## Architecture Overview

This project uses two Databricks Asset Bundles (DABs) deployed in sequence:

```
healthcareFinRecon/
├── deploy.sh                         ← orchestrates both bundles
├── healthcarefinrecon-infra/         ← data plane (pipeline, schema, volumes)
└── healthcarefinrecon-app/           ← semantic layer (metric views, dashboard, genie)
```

### `healthcarefinrecon-infra` — Data Plane

Provisions the Unity Catalog schema, a managed bronze landing volume, and an 8-step Spark Declarative Pipeline (SDP) implementing the medallion architecture:

| Step | Table | Layer | Description |
|------|-------|-------|-------------|
| 01 | `bronze_rc_claims` | Bronze | Raw claims (FHIR JSON / EDI 837) from clearinghouse |
| 02 | `bronze_rc_eob` | Bronze | Raw EOB / ERA 835 remittance records |
| 03 | `silver_rc_claim` | Silver | Cleansed, typed, de-duplicated claims |
| 04 | `silver_rc_eob` | Silver | Cleansed EOB / remittance records |
| 05 | `gold_rc_claim` | Gold | Claims enriched with fiscal period tagging (FY starts July 1) |
| 06 | `gold_rc_eob` | Gold | EOB joined to claim context with variance calculation |
| 07 | `gold_rc_recon_exception` | Gold | Unresolved payment variances with AR aging buckets |
| 08 | `gold_rc_fiscal_calendar` | Gold | Fiscal calendar dimension (P1=July, P12=June) |

### `healthcarefinrecon-app` — Semantic Layer

Deploys metric views, an AI/BI dashboard, and a meta-driven deployer job:

- **Metric Views** (fixtures pattern — YAML files in `fixtures/metric_views/`):
  - `mv_claim_payment_variance` — billed vs paid vs denied variance by payer/provider
  - `mv_denial_rate_by_payer` — denial rates and amounts by payer and reason code
  - `mv_recon_exception_aging` — AR aging buckets for unreconciled exceptions
  - `mv_ar_aging_by_provider` — collection performance by provider
- **AI/BI Dashboard** — Revenue Cycle Reconciliation (placeholder; export from UI)
- **Deployer Job** — `deploy_metric_views.ipynb` discovers all `*.metric_view.yml` fixtures and creates metric views via `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML`

## Deployment

```bash
# Deploy both bundles to the hls_fde target (default)
./deploy.sh

# Deploy to a different target
./deploy.sh <target>
```

After deployment, run the **RC Recon — Deploy Metric Views & Refresh Dashboard** job in the workspace to create/update metric views and refresh the dashboard.

## Adding a Metric View

1. Drop a new `*.metric_view.yml` file in `healthcarefinrecon-app/fixtures/metric_views/`
2. Use `{catalog}` and `{schema}` as Python `.format()` placeholders for the UC catalog/schema
3. Re-run the `deploy_metric_views` job (or notebook directly)

## Variable Convention

- `${var.catalog}` / `${var.schema}` — used **only** in `rc_recon.schema.yml` to define the schema resource
- `${resources.schemas.rc_recon.catalog_name}` / `${resources.schemas.rc_recon.name}` — used everywhere else to cross-reference the deployed schema
