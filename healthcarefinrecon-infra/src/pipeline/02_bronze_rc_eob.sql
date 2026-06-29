-- Bronze: Raw Explanation of Benefits / Remittance Advice (ERA 835)
CREATE OR REFRESH STREAMING TABLE bronze_rc_eob
COMMENT 'Raw inbound EOB / ERA 835 remittance records'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  _metadata.file_path                                 AS _source_file,
  _metadata.file_modification_time                    AS _source_ts,
  current_timestamp()                                 AS _ingest_ts,
  *
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/bronze_landing/eob/',
  format => 'json',
  inferSchema => true,
  includeExistingFiles => true
);
