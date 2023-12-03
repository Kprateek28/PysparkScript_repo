from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ == "__main__":

    spark = SparkSession.builder\
            .appName("IncrementalProcessing")\
            .master("local[2]")\
            .getOrCreate()

    # Define the schema
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Price", IntegerType(), True)
    ])

    # Define the schema for incremental data
    schema_incremental = StructType([
        StructField("inc_id", IntegerType(), True),
        StructField("inc_product_name", StringType(), True),
        StructField("inc_Quantity", IntegerType(), True),
        StructField("inc_Price", IntegerType(), True)
    ])

    # Create the initial DataFrame
    initial_data = [
        (1, "Mobile", 2, 50000),
        (2, "T.V", 1, 60000),
        (3, "HeadPhone", 15, 45000)
    ]
    df = spark.createDataFrame(initial_data, schema=schema)

    # Show the initial DataFrame
    df.show()

    # Create another DataFrame with incremental data (including new and changed records)
    incremental_data = [
        (1, "Mobile", 2, 52000),  # Updated Price for Mobile
        (2, "T.V", 1, 65000),  # Updated Price for T.V
        (3, "HeadPhone", 16, 47000),  # Update product_name,Quantity and Price for Hari
        (4, "Laptop", 1, 70000)  # New record for Laptop
    ]

    # Create a new DataFrame with the incremental data
    incremental_df = spark.createDataFrame(incremental_data, schema=schema_incremental)
    # Show the incremental DataFrame
    # incremental_df.show()

    changed_records = df.join(incremental_df, col("id") == col("inc_id"), 'inner').filter(
        (df.Price != incremental_df.inc_Price) |
        (df.product_name != incremental_df.inc_product_name) |
        (df.Quantity != incremental_df.inc_Quantity)).select("inc_id", "inc_product_name", "inc_Quantity", "inc_Price")
    # changed_records.show()

    # Identify new records in the incremental data
    new_records = incremental_df.join(df, col("id") == col("inc_id"), 'left_outer').filter(df.id.isNull()).select(
        "inc_id", "inc_product_name", "inc_Quantity", "inc_Price")
    # new_records.show()

    updated_df = df.join(
        changed_records.selectExpr("inc_id as id", "inc_product_name ", "inc_Quantity", "inc_Price"),
        'id',
        'left_outer'
    ).coalesce(1). \
        withColumn("product_name", coalesce(col("inc_product_name"), col("product_name"))). \
        withColumn("Quantity", coalesce(col("inc_Quantity"), col("Quantity"))). \
        withColumn("Price", coalesce(col("inc_Price"), col("Price"))). \
        drop("inc_product_name", "inc_Quantity", "inc_Price")

    # updated_df.show()

    final_df = updated_df.union(new_records)
    final_df.show()
    spark.stop()



