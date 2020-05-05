# 501-cloud-computing

Module 2, Big Data Course (81932), University of Bologna

## 501-0 Upload ```dataset/geomarketing``` folder inside your S3 bucket

## 501-1 Create ```geomarketing```  database

### On Athena run:

``` 
CREATE DATABASE geomarketing
```

## 501-2 Create ```ppe``` table inside database ```geomarketing```

``` 
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
```

## 501-3 Create ```pos_potential``` table inside database ```geomarketing```

``` 
CREATE EXTERNAL TABLE geomarketing.pos_potential(
  pos_id bigint, 
  potential bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://<YOUR BUCKET>/geomarketing/pos_potential/'
```

## 501-4 Create ```pos_attributes``` table inside database ```geomarketing```

``` 
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
```

## 501-5 Create ```geo_germany``` table inside database ```geomarketing```

``` 
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
```