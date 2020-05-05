CREATE EXTERNAL TABLE geomarketing.ppe(
  brand string, 
  model string, 
  zipcode bigint, 
  vehicle_qty double, 
  potential double)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://<YOUR BUCKET>/geomarketing/ppe/'