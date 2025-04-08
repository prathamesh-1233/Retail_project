from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("seasonality") \
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
          select *,round(monthly_sale/avg_monthly_sale,2) as seasonality_index
         from
        (
          select *,round(avg(monthly_sale) OVER (PARTITION BY product_id,year),2) as avg_monthly_Sale
          from
            (
                select p.product_id,p.description,p.category,year(td.tran_dt)as year,month(td.tran_dt)as month,sum(td.amt) as monthly_sale
                from product p
                join tran_dtl td on p.product_id=td.product_id
                group by p.product_id,p.description,p.category,year(td.tran_dt),month(td.tran_dt)
            )s
        )s1;   
""")

# Display the DataFrame
result_df.show(5)


# Write seasonality to MySQL
try:
    result_df.write.jdbc(url=jdbc_url, table='seasonality', mode='overwrite', properties=jdbc_properties)
    print("seasonality table written successfully.")
except Exception as e:
    print(f"Error writing seasonality: {e}")


# Stop the SparkSession
spark.stop()
