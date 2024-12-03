from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql.functions import parse_json
import json
import argparse
import logging
import os
import pyspark


def configure_spark_logging():
    logger = logging.getLogger('org.apache.spark')
    logger.setLevel(logging.ERROR)

    logger = logging.getLogger('org.apache.kafka')
    logger.setLevel(logging.ERROR)


def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")
    if not os.access(directory, os.W_OK):
        raise PermissionError(f"No write access to directory: {directory}")


class SchemaHandler:
    def __init__(self, schema_file_path):
        self.schema_file_path = schema_file_path
        self.avro_schema_string = None
        self.spark_schema = None

    def load_schema(self):
        with open(self.schema_file_path, 'r') as schema_file:
            avro_schema = json.load(schema_file)
            self.avro_schema_string = json.dumps(avro_schema)
            self.spark_schema = self.avro_to_spark_schema(avro_schema)
        return self.avro_schema_string, self.spark_schema

    def avro_to_spark_schema(self, avro_schema):
        def convert_field(field):
            avro_type = field['type']
            if isinstance(avro_type, list):
                avro_type = [t for t in avro_type if t != 'null'][0]
            if isinstance(avro_type, dict):
                if avro_type['type'] == 'record':
                    return StructField(field['name'], self.avro_to_spark_schema(avro_type), True)
                elif avro_type['type'] == 'array':
                    return StructField(field['name'], ArrayType(StringType()), True)
            elif avro_type == 'string':
                return StructField(field['name'], StringType(), True)
            elif avro_type == 'int':
                return StructField(field['name'], IntegerType(), True)
            elif avro_type == 'double':
                return StructField(field['name'], DoubleType(), True)
            else:
                return StructField(field['name'], StringType(), True)

        fields = [convert_field(field) for field in avro_schema['fields']]
        return StructType(fields)


class KafkaStreamProcessor:
    def __init__(self, args, schema_handler):
        self.args = args
        self.schema_handler = schema_handler
        self.spark = SparkSession.builder.appName("KafkaReadExample").getOrCreate()

    def write_data(self, result_df, mode, table_path, partition_by=None, tableName=""):
        table_path = f"{table_path}/{tableName}"
        writer = result_df.write.format("delta").mode(mode)

        if partition_by:
            partition_columns = [col.strip() for col in partition_by.split(",")]
            writer = writer.partitionBy(*partition_columns)

        writer.save(table_path)
        print(f"Data written to {table_path}")

    def create_kafka_read_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.args.bootstrap_servers) \
            .option("subscribe", self.args.topic) \
            .option("maxOffsetsPerTrigger", self.args.maxOffsetsPerTrigger) \
            .option("startingOffsets", self.args.startingOffsets) \
            .option("kafka.group.id", "python-consumer-spark") \
            .option("enable.auto.commit", "true") \
            .load()

    def sql_transformer(self, expanded_df, sqlTransformer, sqlTransformerSql):
        if sqlTransformer:
            try:
                columns = sqlTransformerSql.split("SELECT ", 1)[1].split(" FROM")[0].strip()
                result_df = expanded_df.selectExpr(*columns.split(", "))
            except Exception as e:
                print(f"Error executing SQL: {str(e)}")
                raise Exception(e)
        else:
            result_df = expanded_df

        return result_df

    def process_batch(self, batch_df, batch_id):
        parsed_batch_df = batch_df.select(
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            from_json(col("value").cast("string"), self.schema_handler.spark_schema).alias("data")
        )

        expanded_df = parsed_batch_df.select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "data.*"
        )
        print("$$$$$$")
        expanded_df.printSchema()

        result_df = self.sql_transformer(expanded_df, self.args.sqlTransformer, self.args.sqlTransformerSql)
        result_df.show(truncate=True)
        result_df.printSchema()

        if result_df.count() > 0:
            self.write_data(result_df, self.args.mode, self.args.tablePath, self.args.partitionBy, self.args.tableName)

    def start_streaming(self):
        kafka_df = self.create_kafka_read_stream()
        query = kafka_df.writeStream \
            .foreachBatch(self.process_batch) \
            .outputMode("update") \
            .trigger(processingTime=self.args.minSyncInterval) \
            .option("checkpointLocation", self.args.checkpoint) \
            .start()
        query.awaitTermination()


def parse_arguments():
    parser = argparse.ArgumentParser(description="Kafka to Parquet Streaming")
    parser.add_argument("--schema-file", required=True, help="Path to Avro schema file")
    parser.add_argument("--bootstrap-servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--startingOffsets", required=True, help="Starting offsets for Kafka")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--minSyncInterval", default="1 minute", help="Minimum sync interval for trigger")
    parser.add_argument("--sqlTransformer", type=bool, default=False, help="Enable SQL transformer")
    parser.add_argument("--sqlTransformerSql", default="SELECT * FROM SRC", help="SQL query for transformation")
    parser.add_argument("--tablePath", required=True, help="Directory path for Parquet files")
    parser.add_argument("--mode", required=True, help="Currently supports Append")
    parser.add_argument("--partitionBy", required=False, help="partitionBy used for tables ")
    parser.add_argument("--maxOffsetsPerTrigger", required=False, default="1000", help="")
    parser.add_argument("--tableName", required=True, help="Table Name is required ")

    parser.add_argument("--checkpoint", required=True, help="Table Name is required ")

    return parser.parse_args()


def main():
    configure_spark_logging()
    args = parse_arguments()

    # ensure_dir(args.tablePath)

    schema_handler = SchemaHandler(args.schema_file)
    schema_handler.load_schema()

    processor = KafkaStreamProcessor(args, schema_handler)
    processor.start_streaming()


if __name__ == "__main__":
    main()
