from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Affinity_size_3_by_pid") \
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
tran_dtl_df = spark.read.csv(r"C:\prathamesh\loop\spark_project\prathamesh\fact_dimension_table\tran_dtl", header=True,
                             inferSchema=True)
product_df = spark.read.csv(r"C:\prathamesh\loop\spark_project\prathamesh\fact_dimension_table\product", header=True,
                            inferSchema=True)
# C:\d\loop\spark_sql_project\Input_data\product.csv
# Create a temporary view for SQL operations
tran_dtl_df.createOrReplaceTempView("tran_dtl")
product_df.createOrReplaceTempView("product")

# Create views using the temporary view
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW combo_df AS
SELECT
    d1.product_id AS product1,
    d2.product_id AS product2,
    COUNT(d1.tranId) AS combo_count,
    CONCAT(d1.product_id, '_', d2.product_id) AS combination_pair_2
FROM tran_dtl d1
JOIN tran_dtl d2 ON d1.tranId = d2.tranId
WHERE d1.product_id != d2.product_id
GROUP BY d1.product_id, d2.product_id
ORDER BY product1, product2
""")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW triple_combo_count_df AS
SELECT
    d1.product_id AS product1,
    d2.product_id AS product2,
    d3.product_id AS product3,
    COUNT(d1.tranId) AS triple_combo_count,
    CONCAT(d1.product_id, '_', d2.product_id, '_', d3.product_id) AS combination_pair_3
FROM tran_dtl d1
JOIN tran_dtl d2 ON d1.tranId = d2.tranId
JOIN tran_dtl d3 ON d1.tranId = d3.tranId
WHERE d1.product_id != d2.product_id
  AND d1.product_id != d3.product_id
  AND d2.product_id != d3.product_id
GROUP BY d1.product_id, d2.product_id, d3.product_id
ORDER BY product1, product2, product3
""")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW joined_df AS
SELECT
    tc.product1 AS p1,
    tc.product2 AS p2,
    tc.product3 AS p3,
    tc.combination_pair_3,
    c.combo_count,
    tc.triple_combo_count,
    ROUND(tc.triple_combo_count / c.combo_count, 3) AS Affinity_score
FROM triple_combo_count_df tc
LEFT JOIN combo_df c
    ON tc.product1 = c.product1 AND tc.product2 = c.product2
GROUP BY tc.product1, tc.product2, tc.product3, tc.combination_pair_3, c.combo_count, tc.triple_combo_count
ORDER BY tc.product1, tc.product2, tc.product3;

""")

result_df = spark.sql("SELECT * FROM joined_df")
result_df.show(5)


# Write Affinity_size_3_by_pid to MySQL
try:
    result_df.write.jdbc(url=jdbc_url, table='Affinity_size_3_by_pid', mode='overwrite', properties=jdbc_properties)
    print("Affinity_size_3_by_pid table written successfully.")
except Exception as e:
    print(f"Error writing Affinity_size_3_by_pid: {e}")


# Stop the SparkSession
spark.stop()
