from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("pareto") \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.29") \
    .getOrCreate()

# Define database connection properties
jdbc_url = 'jdbc:mysql://localhost:3306/prathamesh'
jdbc_properties = {
    'user': 'root',
    'password': 'root123',
    'driver': 'com.mysql.cj.jdbc.Driver'
}

# Load data from CSV file into Spark DataFrame
tran_dtl_df = spark.read.csv(r"C:\prathamesh\loop\spark_project\prathamesh\fact_dimension_table\tran_dtl", header=True, inferSchema=True)
product_df = spark.read.csv(r"C:\prathamesh\loop\spark_project\prathamesh\fact_dimension_table\product", header=True, inferSchema=True)
# C:\d\loop\spark_sql_project\Input_data\product.csv
# Create a temporary view for SQL operations
tran_dtl_df.createOrReplaceTempView("tran_dtl")
product_df.createOrReplaceTempView("product")

# Execute SQL query on the DataFrame
result_df = spark.sql("""
        WITH sales_per_product AS (
    SELECT
        p.product_id,
        YEAR(t.tran_dt) AS year,
        ROUND(SUM(t.amt), 2) AS total_sale
    FROM tran_dtl t
    JOIN product p ON t.product_id = p.product_id
    GROUP BY p.product_id, YEAR(t.tran_dt)
    ORDER BY total_sale DESC
),
total_sales AS (
    SELECT
        ROUND(SUM(total_sale), 2) AS ts
    FROM sales_per_product
),
cumulative_sales AS (
    SELECT
        product_id,
        year,
        total_sale,
        ROUND(SUM(total_sale) OVER (ORDER BY total_sale DESC), 2) AS cumulative_total
    FROM sales_per_product
),
pareto_chart AS (
    SELECT
        product_id,
        year,
        total_sale,
        cumulative_total,
        ROUND((cumulative_total / ts * 100), 2) AS cumulative_percentage
    FROM cumulative_sales, total_sales
)
SELECT
    product_id,
    year,
    total_sale,
    cumulative_total,
    cumulative_percentage
FROM pareto_chart
ORDER BY cumulative_percentage ASC;
""")

# Display the DataFrame
result_df.show(5)

# Write pareto to MySQL
try:
    result_df.write.jdbc(url=jdbc_url, table='pareto', mode='overwrite', properties=jdbc_properties)
    print("pareto table written successfully.")
except Exception as e:
    print(f"Error writing pareto: {e}")


# Stop the SparkSession
spark.stop()
