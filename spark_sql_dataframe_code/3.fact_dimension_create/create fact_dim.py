from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("create fact and dimensions table") \
    .config("spark.jars", r"C:\Users\philo\.ivy2\jars\mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

# Define database connection properties
jdbc_url = 'jdbc:mysql://localhost:3306/prathamesh'
jdbc_properties = {
    'user': 'root',
    'password': 'root123',
    'driver': 'com.mysql.cj.jdbc.Driver'
}

# Load CSV data
path = r"./floo"
df1 = spark.read.csv(path, header=True, inferSchema=True)

# Transaction Header
tran_hdr = df1.select("tranId", "store_id", "member_id", "tran_dt").dropDuplicates(["tranId"])
tran_hdr.show(5)

# Write Transaction Header to MySQL
try:
    tran_hdr.write.jdbc(url=jdbc_url, table='tran_hdr', mode='overwrite', properties=jdbc_properties)
    print("Transaction Header table written successfully.")
except Exception as e:
    print(f"Error writing Transaction Header: {e}")

# Transaction Detail
tran_dtl = df1.select("tranId", "product_id", "qty", "amt", "tran_dt").dropDuplicates(["tranId", "product_id"])
try:
    tran_dtl.write.jdbc(url=jdbc_url, table='tran_dtl', mode='overwrite', properties=jdbc_properties)
    print("Transaction Detail table written successfully.")
except Exception as e:
    print(f"Error writing Transaction Detail: {e}")

# Member Table
member = df1.select("member_id", "fs_name", "ls_name", "store_name").dropDuplicates(["member_id"])
try:
    member.write.jdbc(url=jdbc_url, table='member', mode='overwrite', properties=jdbc_properties)
    print("Member table written successfully.")
except Exception as e:
    print(f"Error writing Member table: {e}")

# Product Table
product = df1.select("product_id", "description", "category", "price").dropDuplicates(["product_id"])
try:
    product.write.jdbc(url=jdbc_url, table='product', mode='overwrite', properties=jdbc_properties)
    print("Product table written successfully.")
except Exception as e:
    print(f"Error writing Product table: {e}")

# Stop the Spark session
spark.stop()
