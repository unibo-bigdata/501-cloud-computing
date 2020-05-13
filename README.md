# 501-cloud-computing

Module 2, Big Data Course (81932), University of Bologna

## 501-0 Upload ```dataset/geomarketing``` folder inside your S3 bucket

## 501-1 Create ```geomarketing```  database

### On Athena run:

``` 
CREATE DATABASE geomarketing
```

## 501-2 Create ```ppe``` table inside database ```geomarketing```

### On Athena run:

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

### On Athena run:

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

### On Athena run:

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

### On Athena run:

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

## 501-6 EMR Data Flow

### Initialize SQL Context

``` 
val sqlContext = spark.sqlContext
```

### Load Sources

``` 
val posAttrDF = sqlContext.sql("select * from geomarketing.pos_attributes where flag = 'Keep' ")

posAttrDF.createOrReplaceTempView("pos_attr")

val posPotDF = sqlContext.sql("select * from geomarketing.pos_potential")

posPotDF.createOrReplaceTempView("pos_pot")

val ppeDF = sqlContext.sql("select * from geomarketing.ppe")

ppeDF.createOrReplaceTempView("ppe")

val geoGermanyDF = sqlContext.sql("select * from geomarketing.geo_germany")

geoGermanyDF.createOrReplaceTempView("geography")
```

### Calculate the total potential for each zipcode

``` 
val landkreisPotentialDF = sqlContext.sql("select landkreis, sum(vehicle_qty) vehicle_qty,sum(potential) tot_potential from ppe x join geography y on x.zipcode = y.zipcode group by landkreis")

landkreisPotentialDF.createOrReplaceTempView("total_pot_landkreis")
```

### Calculate PoS potential for each zipcode

``` 
val posPotAllocatedDF = sqlContext.sql("select landkreis,sum(potential) alloc_potential from pos_pot a join pos_attr b on a.pos_id = b.pos_id join geography c on b.zipcode = c.zipcode where flag = 'Keep' group by landkreis")

posPotAllocatedDF.createOrReplaceTempView("tot_pot_allocated")
```

### Calculate the potential to allocate, per zipcode

``` 
val potToAllocateDF = sqlContext.sql("select a.landkreis,sum(a.tot_potential-b.alloc_potential) potential_left from total_pot_landkreis a left join tot_pot_allocated b on a.landkreis = b.landkreis  group by a.landkreis")

potToAllocateDF.createOrReplaceTempView("potential_to_allocate_landkreis")
```
### Calculate average by subtypology

``` 
val avgSubtypologyDF = sqlContext.sql("select landkreis,subtypology,sum(potential) / count(a.pos_id) subtypology_avg from pos_pot a join pos_attr b on a.pos_id = b.pos_id join geography c on b.zipcode = c.zipcode where flag = 'Keep' group by landkreis,subtypology order by 1,2")

avgSubtypologyDF.createOrReplaceTempView("avgSubtypology")

val totAvgSubtypologyDF = sqlContext.sql("select landkreis,sum(subtypology_avg) totAvgSubtypology from avgSubtypology group by landkreis order by 1")

totAvgSubtypologyDF.createOrReplaceTempView("totAvgSubtypology")

val weightDF = sqlContext.sql("select x.landkreis,subtypology,(subtypology_avg/totAvgSubtypology) weightLandkreis  from avgSubtypology x join totAvgSubtypology y on x.landkreis = y.landkreis ")

weightDF.createOrReplaceTempView("weight")
```

### Compute and wright final result

``` 
val potentialAllocationDF = sqlContext.sql("select  pos_code,g.landkreis,a.subtypology,case when potential = 0 then weightLandkreis*potential_left else potential end as final_potential from  pos_attr a join pos_pot b on a.pos_id = b.pos_id join geography g on a.zipcode = g.zipcode join weight w on g.landkreis = w.landkreis and a.subtypology = w.subtypology join potential_to_allocate_landkreis p on g.landkreis = p.landkreis  where flag = 'Keep' order by 2,1")

potentialAllocationDF.write.option("path","s3://<YOUR BUCKET>/geomarketing/pos_final/").mode("append").saveAsTable("geomarketing.pos_final")
```