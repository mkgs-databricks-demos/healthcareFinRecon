-- Gold: Enriched claims with fiscal calendar
CREATE OR REFRESH LIVE TABLE gold_rc_claim
COMMENT 'Enriched claims with fiscal period tagging (FY starts July 1)'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  c.*,
  -- Fiscal year: starts July 1. July 2024 = FY2025
  CASE
    WHEN MONTH(c.service_date) >= 7 THEN YEAR(c.service_date) + 1
    ELSE YEAR(c.service_date)
  END                                                   AS fiscal_year,
  -- Fiscal period: July = P1, Aug = P2, ..., June = P12
  CASE
    WHEN MONTH(c.service_date) >= 7 THEN MONTH(c.service_date) - 6
    ELSE MONTH(c.service_date) + 6
  END                                                   AS fiscal_period
FROM LIVE.silver_rc_claim c;
