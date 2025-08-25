import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

def create_spark_session():
    """Initialize Spark Session."""
    return SparkSession.builder.appName("FashionBoutiqueTransform").getOrCreate() 

def load_and_clean(spark, input_file, output_dir):
    """
    Stage 1: Load dataset, drop duplicates, remove nulls, save cleaned data.
    """

    # Define schema (adjust based on your dataset columns)
    schema = T.StructType([
        T.StructField("product_id", T.StringType(), False),
        T.StructField("product_name", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("subcategory", T.StringType(), True),
        T.StructField("price", T.FloatType(), True),
        T.StructField("stock", T.IntegerType(), True),
        T.StructField("brand", T.StringType(), True),
        T.StructField("color", T.StringType(), True),
        T.StructField("size", T.StringType(), True),
        T.StructField("rating", T.FloatType(), True),
        T.StructField("reviews", T.IntegerType(), True)
    ])

    df = spark.read.schema(schema).csv(input_file, header=True)

    # Clean: remove duplicates + null product_id
    df = df.dropDuplicates(["product_id"]).filter(F.col("product_id").isNotNull())

    # Fill nulls for numeric/text fields
    df = df.fillna({"price": 0.0, "stock": 0, "rating": 0.0, "reviews": 0})
    df = df.fillna("Unknown")

    df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "cleaned_data"))
    print("Stage 1: Cleaned data saved")

    return df

def create_master_table(output_dir, df):
    """Stage 2: Create master product table with standardized fields."""

    # Example: normalize names & categories
    master_df = df.withColumn("product_name", F.initcap(F.col("product_name"))) \
                  .withColumn("category", F.upper(F.col("category"))) \
                  .withColumn("subcategory", F.upper(F.col("subcategory")))

    master_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage2", "master_table"))
    print("Stage 2: Master table saved")
    return master_df

def create_query_tables(output_dir, df):
    """Stage 3: Create query-optimized tables."""

    # Product metadata table
    product_metadata = df.select("product_id", "product_name", "category", "subcategory", "brand", "color", "size")
    product_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "product_metadata"))

    # Pricing & stock table
    pricing_stock = df.select("product_id", "price", "stock", "rating", "reviews")
    pricing_stock.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "pricing_stock"))

    print("Stage 3: Query-optimized tables saved")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python fashion_etl.py <input_file.csv> <output_dir>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    spark = create_spark_session()

    df = load_and_clean(spark, input_file, output_dir)
    master_df = create_master_table(output_dir, df)
    create_query_tables(output_dir, df)

    print("Transformation pipeline completed")
