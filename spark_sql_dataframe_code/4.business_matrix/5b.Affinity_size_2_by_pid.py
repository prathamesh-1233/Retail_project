from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Affinity_size_2_by_pid") \
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

# Execute SQL query on the DataFrame
result_df = spark.sql("""

                select product1, product2,concat(product1,"_",product2) as combination_pair,combo_count, count as product1_count,round(combo_count/count,3) as affinity_score
                from
                   (
                    select d1.product_id as product1 ,d2.product_id as product2,count(d1.tranId) as combo_count from tran_dtl d1
                    join tran_dtl d2 on d1.tranId=d2.tranId
                    where d1.product_id!=d2.product_id  
                    group by d1.product_id,d2.product_id 
                    order BY d1.product_id, d2.product_id
                    )a
                join ( select product_id,count(product_id) as count from tran_dtl
                   group by product_id)b
                   on a.product1=b.product_id
                   ORDER BY a.product1, a.product2;

""")

# Display the DataFrame
result_df.show(5)


# Write Affinity_size_2_by_pid to MySQL
try:
    result_df.write.jdbc(url=jdbc_url, table='Affinity_size_2_by_pid', mode='overwrite', properties=jdbc_properties)
    print("Affinity_size_2_by_pid table written successfully.")
except Exception as e:
    print(f"Error writing Affinity_size_2_by_pid: {e}")


# Stop the SparkSession
spark.stop()
