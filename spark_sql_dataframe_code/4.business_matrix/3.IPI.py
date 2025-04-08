from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ipi") \
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
tran_hdr_df = spark.read.csv(r"C:\prathamesh\loop\spark_project\prathamesh\fact_dimension_table\tran_hdr", header=True, inferSchema=True)

# Create a temporary view for SQL operations
tran_hdr_df.createOrReplaceTempView("tran_hdr")

# Execute SQL query on the DataFrame
result_df = spark.sql("""
    WITH previous_transactions AS (
    SELECT
        member_id,
        tran_dt,
        LAG(tran_dt) OVER (PARTITION BY member_id ORDER BY tran_dt) AS previous_transaction
    FROM tran_hdr th
),
interpurchase_intervals AS (
    SELECT
        member_id,
        tran_dt,
        previous_transaction,
        DATEDIFF(tran_dt, previous_transaction) AS interpurchase_interval
    FROM previous_transactions
    WHERE previous_transaction IS NOT NULL
),
average_interpurchase_interval AS (
    SELECT
        member_id,
        ROUND(AVG(interpurchase_interval), 2) AS average_interpurchase_interval
    FROM interpurchase_intervals
    GROUP BY member_id
)
SELECT
    i.member_id,
    i.previous_transaction,
    i.tran_dt,
    i.interpurchase_interval,
    a.average_interpurchase_interval
FROM interpurchase_intervals i
JOIN average_interpurchase_interval a
ON i.member_id = a.member_id
ORDER BY i.member_id, i.tran_dt
""")

# Display the DataFrame
result_df.show(5)


# Write ipi to MySQL
try:
    result_df.write.jdbc(url=jdbc_url, table='ipi', mode='overwrite', properties=jdbc_properties)
    print("ipi table written successfully.")
except Exception as e:
    print(f"Error writing ipi: {e}")



# Stop the SparkSession
spark.stop()
