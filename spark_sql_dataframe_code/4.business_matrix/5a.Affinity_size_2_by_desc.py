from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Affinity_size_2_by_desc") \
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
SELECT 
    p1.description AS product1_description,
    p2.description AS product2_description,
    CONCAT(p1.description, "_", p2.description) AS combination_pair,
    combo_count,
    count AS product1_count,
    ROUND(combo_count / count, 3) AS affinity_score
FROM
    (
        SELECT 
            d1.product_id AS product1,
            d2.product_id AS product2,
            COUNT(d1.tranId) AS combo_count
        FROM tran_dtl d1
        JOIN tran_dtl d2 ON d1.tranId = d2.tranId
        WHERE d1.product_id != d2.product_id
        GROUP BY d1.product_id, d2.product_id
        ORDER BY d1.product_id, d2.product_id
    ) a
JOIN 
    (
        SELECT 
            product_id,
            COUNT(product_id) AS count
        FROM tran_dtl
        GROUP BY product_id
    ) b ON a.product1 = b.product_id
JOIN 
    product p1 ON a.product1 = p1.product_id
JOIN 
    product p2 ON a.product2 = p2.product_id
ORDER BY 
    a.product1, a.product2;
"""
)
# Display the DataFrame
result_df.show(5)


# Write Affinity_size_2_by_desc to MySQL
try:
    result_df.write.jdbc(url=jdbc_url, table='Affinity_size_2_by_desc', mode='overwrite', properties=jdbc_properties)
    print("Affinity_size_2_by_desc table written successfully.")
except Exception as e:
    print(f"Error writing Affinity_size_2_by_desc: {e}")



# Stop the SparkSession
spark.stop()
