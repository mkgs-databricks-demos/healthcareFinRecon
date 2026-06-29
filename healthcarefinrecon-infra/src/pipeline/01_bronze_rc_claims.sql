-- Bronze: Raw claims data (CMS-1500 / UB-04 equivalent)
-- Ingested from FHIR JSON or EDI 837P/837I files uploaded to the bronze volume
CREATE OR REFRESH STREAMING TABLE bronze_rc_claims
COMMENT 'Raw inbound claim records from clearinghouse / source EHR system'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  _metadata.file_path                                 AS _source_file,
  _metadata.file_modification_time                    AS _source_ts,
  current_timestamp()                                 AS _ingest_ts,
  *
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/bronze_landing/claims/',
  format => 'json',
  inferSchema => true,
  includeExistingFiles => true
);
