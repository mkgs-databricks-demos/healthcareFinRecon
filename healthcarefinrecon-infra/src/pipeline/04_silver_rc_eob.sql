-- Silver: Cleansed EOB / remittance records
CREATE OR REFRESH LIVE TABLE silver_rc_eob
COMMENT 'Cleansed and typed EOB / ERA 835 remittance records'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY claim_id, eob_id
      ORDER BY _source_ts DESC
    ) AS rn
  FROM LIVE.bronze_rc_eob
  WHERE claim_id IS NOT NULL
)
SELECT
  CAST(eob_id              AS STRING)                  AS eob_id,
  CAST(claim_id            AS STRING)                  AS claim_id,
  CAST(payer_id            AS STRING)                  AS payer_id,
  TRY_CAST(payment_date    AS DATE)                    AS payment_date,
  TRY_CAST(paid_amount     AS DECIMAL(18,2))           AS paid_amount,
  TRY_CAST(denied_amount   AS DECIMAL(18,2))           AS denied_amount,
  TRY_CAST(adjusted_amount AS DECIMAL(18,2))           AS adjusted_amount,
  CAST(adjudication_status AS STRING)                  AS adjudication_status,  -- paid / denied / partial / pending
  CAST(denial_reason_code  AS STRING)                  AS denial_reason_code,
  CAST(denial_reason_desc  AS STRING)                  AS denial_reason_desc,
  _ingest_ts,
  _source_file
FROM deduped
WHERE rn = 1;
