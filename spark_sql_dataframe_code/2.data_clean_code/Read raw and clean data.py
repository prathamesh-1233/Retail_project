from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, lit, bround, initcap, to_date

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Read CSV and clean data") \
    .getOrCreate()

# Path
path = r"C:\prathamesh\loop\spark_project\data\mergedtransactionsData.csv"
# path = r"C:\d\loop\spark_project\data\complete.csv"‪
# Read the CSV file into a DataFrame

df = spark.read.csv(path, header=True, inferSchema=True)
df1 = df.withColumnRenamed("start", "tran_dt") \
    .withColumn("tran_dt", to_date("tran_dt", "dd-MM-yyyy")) \
    .withColumn("member_name", concat_ws(" ", col("fs_name"), col("ls_name"))) \
    .withColumn("amt", (col("qty") * col("price"))) \
    .withColumn("amt", bround("amt", 2)) \
    .withColumn("fs_name", initcap(col("fs_name"))) \
    .withColumn("ls_name", initcap(col("ls_name"))) \
    .withColumn("member_name", initcap(col("member_name"))) \
    .withColumn("store_name", initcap(col("store_name"))) \
    .withColumn("member_id", col("member_id").cast("int")) \
    .withColumn("product_id", col("product_id").cast("int")) \
    .withColumn("store_id", col("store_id").cast("int"))

# Path to save the cleaned DataFrame as CSV
output_path = r"./floo"
df1 = df1.dropna()


# Write cleaned DataFrame to a CSV file
try:
    df1.coalesce(1).write.csv(output_path, header=True, mode='overwrite')
    print("raw data written successfully to csv.")
except Exception as e:
    print(f"Error writing raw data to csv: {e}")


spark.stop()