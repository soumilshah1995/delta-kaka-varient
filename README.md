# delta-kaka-varient
delta-kaka-varient

![output](https://github.com/user-attachments/assets/e7bbe9f5-1d1f-493f-ad87-f8479cdd3535)


# blog 
https://www.linkedin.com/pulse/learn-how-ingest-semi-structured-data-from-kafka-topics-soumil-shah-6okxe/?trackingId=UwHTgiaiSkK8FkBLAxdFAw%3D%3D

# job
```
spark-submit \
    --packages 'io.delta:delta-spark_2.13:4.0.0rc1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview1,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.773' \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"  \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.hadoop.fs.s3a.path.style.access=S3SignerType" \
    /Users/sshah/IdeaProjects/poc-projects/soumil-poc/delta-lake-poc/template.py \
    --schema-file /Users/sshah/IdeaProjects/poc-projects/lakehouse/poc/schemaFile/customer.avsc \
    --bootstrap-servers "localhost:7092"  \
    --topic customers \
    --startingOffsets earliest \
    --sqlTransformer true \
    --sqlTransformerSql "SELECT ts AS event_time, emp_id, employee_name, department, state, salary, age, bonus, parse_json(to_json(properties)) AS properties, year(current_date()) AS current_year, month(current_date()) AS current_month, day(current_date()) AS current_day, kafka_topic, kafka_partition FROM SRC" \
    --minSyncInterval "1 minutes" \
    --mode append \
    --partitionBy "kafka_topic" \
    --tablePath file:////Users/sshah/IdeaProjects/poc-projects/lakehouse/poc/warehouse/  \
    --maxOffsetsPerTrigger 10000 \
    --tableName bronze_customers \
    --checkpoint file:////Users/sshah/IdeaProjects/poc-projects/lakehouse/poc/checkpoints/
```
