CREATE EXTERNAL TABLE geomarketing.geo_germany(
  id bigint, 
  land string, 
  landkreis string, 
  city string, 
  zipcode bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://<YOUR BUCKET>/geomarketing/geo_germany/'