CREATE EXTERNAL TABLE geomarketing.pos_attributes(
  pos_id bigint, 
  pos_code bigint, 
  pos_name string, 
  zipcode bigint, 
  city string, 
  country string, 
  macro_typology string, 
  typology string, 
  subtypology string, 
  flag string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://<YOUR BUCKET>/geomarketing/pos_attributes/'