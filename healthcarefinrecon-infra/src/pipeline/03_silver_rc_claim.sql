-- Silver: Cleansed, typed, de-duplicated claims
CREATE OR REFRESH LIVE TABLE silver_rc_claim
COMMENT 'Cleansed and typed claim records; PII minimized'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY claim_id
      ORDER BY _source_ts DESC
    ) AS rn
  FROM LIVE.bronze_rc_claims
  WHERE claim_id IS NOT NULL
)
SELECT
  CAST(claim_id         AS STRING)                     AS claim_id,
  CAST(patient_id       AS STRING)                     AS patient_id,
  CAST(provider_npi     AS STRING)                     AS provider_npi,
  CAST(provider_organization AS STRING)                AS provider_organization,
  CAST(payer_id         AS STRING)                     AS payer_id,
  CAST(payer_name       AS STRING)                     AS payer_name,
  CAST(claim_type       AS STRING)                     AS claim_type,   -- professional / institutional
  TRY_CAST(service_date AS DATE)                       AS service_date,
  TRY_CAST(submission_date AS DATE)                    AS submission_date,
  TRY_CAST(billed_amount AS DECIMAL(18,2))             AS billed_amount,
  CAST(procedure_code   AS STRING)                     AS procedure_code,
  CAST(diagnosis_code   AS STRING)                     AS diagnosis_code,
  CAST(place_of_service AS STRING)                     AS place_of_service,
  _ingest_ts,
  _source_file
FROM deduped
WHERE rn = 1
  AND service_date IS NOT NULL;
