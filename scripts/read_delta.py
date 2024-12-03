try:
    import pyspark
    from delta import *
    from pyspark.sql.functions import parse_json
    from pyspark.sql.functions import try_variant_get
except Exception as e:
    print("Error ", e)

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read data from the Delta table
delta_table_path = "/Users/sshah/IdeaProjects/poc-projects/lakehouse/poc/warehouse/bronze_customers"
delta_df = spark.read.format("delta").load(delta_table_path)



df3 = delta_df.select(
    try_variant_get("properties", "$.address.city", "STRING").alias("address_city"),
    try_variant_get("properties", "$.employment.position", "STRING").alias("employment_position"),
    try_variant_get("properties", "$.contacts.email", "STRING").alias("contact_email"),
    try_variant_get("properties", "$.preferences.newsletter_subscribed", "BOOLEAN").alias("newsletter_subscribed"),
    try_variant_get("properties", "$.preferences.preferred_language", "STRING").alias("preferred_language")
)

# Show the extracted values
df3.show()


