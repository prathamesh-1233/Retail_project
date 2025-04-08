from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count, month, min, max

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("trip_count") \
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
tran_hdr_df = spark.read.csv(r"C:\prathamesh\loop\spark_project\prathamesh\fact_dimension_table\tran_hdr", header=True,
                             inferSchema=True)

# Create a temporary view for SQL operations
tran_hdr_df.createOrReplaceTempView("tran_hdr")

# Execute SQL query on the DataFrame
result_df = spark.sql("""

select member_id,year(tran_dt)as year,count(tranId)as trip_count
from tran_hdr
group by member_id,year
order by member_id,year;
""")

# Display the DataFrame
result_df.show(5)


# Write trip_count to MySQL
try:
    result_df.write.jdbc(url=jdbc_url, table='trip_count', mode='overwrite', properties=jdbc_properties)
    print("trip_count table written successfully.")
except Exception as e:
    print(f"Error writing trip_count: {e}")



# Stop the SparkSession
spark.stop()
