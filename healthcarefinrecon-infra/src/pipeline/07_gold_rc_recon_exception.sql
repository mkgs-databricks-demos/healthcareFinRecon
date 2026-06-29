-- Gold: Reconciliation exceptions — claims with open, partial, or unexplained variances
CREATE OR REFRESH LIVE TABLE gold_rc_recon_exception
COMMENT 'Claims with unresolved payment variance requiring reconciliation action'
TBLPROPERTIES ('quality' = 'gold')
AS
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
  COALESCE(e.paid_amount,   0.00)                       AS paid_amount,
  COALESCE(e.denied_amount, 0.00)                       AS denied_amount,
  c.billed_amount - COALESCE(e.paid_amount, 0) - COALESCE(e.denied_amount, 0)  AS variance_amount,
  COALESCE(e.adjudication_status, 'unsubmitted')        AS adjudication_status,
  e.denial_reason_code,
  e.denial_reason_desc,
  -- Days since service
  DATEDIFF(current_date(), c.service_date)              AS days_outstanding,
  -- AR aging bucket
  CASE
    WHEN DATEDIFF(current_date(), c.service_date) <= 30  THEN '0-30'
    WHEN DATEDIFF(current_date(), c.service_date) <= 60  THEN '31-60'
    WHEN DATEDIFF(current_date(), c.service_date) <= 90  THEN '61-90'
    WHEN DATEDIFF(current_date(), c.service_date) <= 120 THEN '91-120'
    ELSE '120+'
  END                                                   AS ar_aging_bucket,
  current_timestamp()                                   AS _gold_ts
FROM LIVE.gold_rc_claim c
LEFT JOIN LIVE.silver_rc_eob e ON c.claim_id = e.claim_id
WHERE
  -- Include: claims with no EOB, partial payment, denial, or meaningful variance
  e.claim_id IS NULL
  OR e.adjudication_status IN ('pending', 'partial', 'denied')
  OR ABS(c.billed_amount - COALESCE(e.paid_amount, 0) - COALESCE(e.denied_amount, 0)) > 0.01;
