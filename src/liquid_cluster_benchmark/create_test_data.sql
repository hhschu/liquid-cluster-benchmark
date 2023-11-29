/* Create a normal table */
CREATE OR REPLACE TABLE datadev.test_ds2023q4.telemetry_v1_classic
USING DELTA
PARTITIONED BY (business_date)
LOCATION 's3://wallbox-datalake-sv-datadev/test_ds2023q4/telemetry_v1_classic/successful/1/part1' AS (
  SELECT
    *
  FROM
    dataprod.cn_charger.telemetry_v1
  WHERE
    business_date in (
      20231120,
      20231121,
      20231122,
      20231123,
      20231124,
      20231125,
      20231126,
      20231127
    )
);

OPTIMIZE datadev.test_ds2023q4.telemetry_v1_classic ZORDER by charger_id;

/* Create a Liquid clustered table */
CREATE EXTERNAL TABLE datadev.test_ds2023q4.telemetry_v1_liquid
CLUSTER BY (charger_id, business_ts)
LOCATION 's3://wallbox-datalake-sv-datadev/test_ds2023q4/telemetry_v1_liquid/successful/1/part1'
AS SELECT * FROM datadev.test_ds2023q4.telemetry_v1_classic;

OPTIMIZE datadev.test_ds2023q4.telemetry_v1_liquid;
