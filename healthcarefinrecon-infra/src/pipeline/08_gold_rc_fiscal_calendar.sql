-- Gold: Fiscal calendar dimension (healthcare fiscal year starts July 1)
CREATE OR REFRESH LIVE TABLE gold_rc_fiscal_calendar
COMMENT 'Healthcare fiscal calendar: FY starts July 1. P1=July, P12=June.'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH date_spine AS (
  SELECT EXPLODE(SEQUENCE(
    DATE '2020-07-01',
    DATE '2030-06-30',
    INTERVAL 1 DAY
  )) AS calendar_date
)
SELECT
  calendar_date,
  YEAR(calendar_date)                                     AS calendar_year,
  MONTH(calendar_date)                                    AS calendar_month,
  -- Fiscal year: month >= 7 → next calendar year
  CASE
    WHEN MONTH(calendar_date) >= 7 THEN YEAR(calendar_date) + 1
    ELSE YEAR(calendar_date)
  END                                                     AS fiscal_year,
  -- Fiscal period: July=1, Aug=2, ..., June=12
  CASE
    WHEN MONTH(calendar_date) >= 7 THEN MONTH(calendar_date) - 6
    ELSE MONTH(calendar_date) + 6
  END                                                     AS fiscal_period,
  -- Fiscal period label
  CONCAT('P',
    LPAD(CAST(CASE
      WHEN MONTH(calendar_date) >= 7 THEN MONTH(calendar_date) - 6
      ELSE MONTH(calendar_date) + 6
    END AS STRING), 2, '0')
  )                                                       AS fiscal_period_label,
  -- Fiscal quarter
  CEIL(CASE
    WHEN MONTH(calendar_date) >= 7 THEN MONTH(calendar_date) - 6
    ELSE MONTH(calendar_date) + 6
  END / 3.0)                                              AS fiscal_quarter,
  DATE_FORMAT(calendar_date, 'MMMM yyyy')                 AS month_name,
  DAYOFWEEK(calendar_date)                                AS day_of_week,
  CASE WHEN DAYOFWEEK(calendar_date) IN (1, 7) THEN true ELSE false END AS is_weekend
FROM date_spine;
