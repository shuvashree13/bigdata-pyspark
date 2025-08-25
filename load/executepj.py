import sys
import os
import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession

def create_spark_session():
    """Initialize Spark session with PostgreSQL JDBC driver."""
    return SparkSession.builder \
        .appName("FashionBoutiqueLoad") \
        .getOrCreate()

def create_postgres_tables():
    """Create PostgreSQL tables if they don't exist using psycopg2."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="123shuva",   # change if needed
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS master_table (
                product_id VARCHAR(50) PRIMARY KEY,
                product_name TEXT,
                category TEXT,
                subcategory TEXT,
                price FLOAT,
                stock INT,
                brand TEXT,
                color TEXT,
                size TEXT,
                rating FLOAT,
                reviews INT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS product_metadata (
                product_id VARCHAR(50),
                product_name TEXT,
                category TEXT,
                subcategory TEXT,
                brand TEXT,
                color TEXT,
                size TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS pricing_stock (
                product_id VARCHAR(50),
                price FLOAT,
                stock INT,
                rating FLOAT,
                reviews INT
            );
            """
        ]

        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()
        print("PostgreSQL tables created successfully")

    except Exception as e:
        print(f"Error creating tables: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_to_postgres(spark, input_dir):
    """Load Parquet files to PostgreSQL."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": "postgres",
        "password": "123shuva",  # change if needed
        "driver": "org.postgresql.Driver"
    }

    tables = [
        ("stage2/master_table", "master_table"),
        ("stage3/product_metadata", "product_metadata"),
        ("stage3/pricing_stock", "pricing_stock")
    ]

    for parquet_path, table_name in tables:
        try:
            df = spark.read.parquet(os.path.join(input_dir, parquet_path))
            mode = "overwrite"  # overwrite to keep tables clean
            df.write \
                .mode(mode) \
                .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            print(f"Loaded {table_name} to PostgreSQL")
        except Exception as e:
            print(f"Error loading {table_name}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python load_fashion.py <input_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]

    if not os.path.exists(input_dir):
        print(f"Error: Input directory {input_dir} does not exist")
        sys.exit(1)

    spark = create_spark_session()
    create_postgres_tables()
    load_to_postgres(spark, input_dir)

    print("Load stage completed")
