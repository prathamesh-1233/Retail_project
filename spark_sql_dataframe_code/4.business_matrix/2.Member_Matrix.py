from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("member_matrix") \
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
    select member_id,yearly_trip_count,membership,
case when membership="Platinum" then "15%"
     when membership="Gold" then "10%" 
     when membership="silver" then "5%" 
     when membership="Bronze" then "No_discount" 
end as member_discount
from
(select member_id,yearly_trip_count,
case when yearly_trip_count>=270 then "Platinum"
     when yearly_trip_count<270 and yearly_trip_count>=250 then "Gold"
     when yearly_trip_count<250 and yearly_trip_count>=230 then"silver"
     else 'Bronze'
end as membership
from 
(select member_id,count(tranId)as yearly_trip_count
from tran_hdr
group by member_id)pk)hg;
""")

# Display the DataFrame
result_df.show(5)



# Write member_matrix to MySQL
try:
    result_df.write.jdbc(url=jdbc_url, table='member_matrix', mode='overwrite', properties=jdbc_properties)
    print("member_matrix table written successfully.")
except Exception as e:
    print(f"Error writing member_matrix: {e}")


# Stop the SparkSession
spark.stop()
