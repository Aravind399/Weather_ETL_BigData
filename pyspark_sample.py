from pyspark.sql import SparkSession

def get_pyspark_version():
    # Create a Spark session without an application name
    spark = SparkSession.builder.getOrCreate()
    return spark.version

if __name__ == "__main__":
    print(get_pyspark_version())



from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

# Assign SparkContext to 'sc'
sc = spark.sparkContext

# Now you can use 'sc' for RDD operations or use 'spark' for DataFrame operations

print('.................................-------------------------------------------------------------------')
current = spark.read.parquet('/home/aravind/apache_airflow/current_data/*',multiLine = True)

forecast = spark.read.parquet('/home/aravind/apache_airflow/forcast_data/*',multiLine = True)

location = spark.read.parquet('/home/aravind/apache_airflow/location_data/*',multiLine = True)


current.write \
  .format("jdbc") \
  .option("driver", "com.mysql.jdbc.Driver") \
  .option("url", "jdbc:mysql://localhost:3306/data_eng") \
  .option("dbtable", "current") \
  .option("user", "root") \
  .option("password", "root") \
  .mode("append") \
  .save()
  
  
  
  
forecast.write \
  .format("jdbc") \
  .option("driver", "com.mysql.jdbc.Driver") \
  .option("url", "jdbc:mysql://localhost:3306/data_eng") \
  .option("dbtable", "forecast") \
  .option("user", "root") \
  .option("password", "root") \
  .mode("append") \
  .save()
  
  
  
location.write \
  .format("jdbc") \
  .option("driver", "com.mysql.jdbc.Driver") \
  .option("url", "jdbc:mysql://localhost:3306/data_eng") \
  .option("dbtable", "location") \
  .option("user", "root") \
  .option("password", "root") \
  .mode("append") \
  .save()


print('**************************************** load successfull ***************************************************************')





