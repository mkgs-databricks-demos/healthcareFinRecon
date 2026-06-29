-- Gold: EOB joined to claim context for variance analysis
CREATE OR REFRESH LIVE TABLE gold_rc_eob
COMMENT 'EOB records enriched with claim context and variance calculation'
TBLPROPERTIES ('quality' = 'gold')
AS
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
  -- Variance: billed - (paid + denied)
  c.billed_amount - COALESCE(e.paid_amount, 0) - COALESCE(e.denied_amount, 0)  AS variance_amount
FROM LIVE.silver_rc_eob e
INNER JOIN LIVE.gold_rc_claim c ON e.claim_id = c.claim_id;
