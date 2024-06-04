
from pyspark.sql import SparkSession

def get_pyspark_version():
    # Create a Spark session without an application name
    spark = SparkSession.builder.getOrCreate()
    return spark.version

if __name__ == "__main__":
    print(get_pyspark_version())



from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import datetime


# Create a Spark session
spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

# Assign SparkContext to 'sc'
sc = spark.sparkContext

time_now  = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M') 
from pyspark.sql.functions import concat, lit
from datetime import datetime




df = spark.read.parquet('/home/aravind/apache_airflow/forcast_data/*')

data=df.select('date','date_epoch','avghumidity','avgtemp_c','avgtemp_f','avgvis_km','avgvis_miles','text','daily_chance_of_rain','daily_will_it_rain','maxtemp_c','maxtemp_f','maxwind_mph','maxwind_kph','mintemp_c','mintemp_f','uv')
dff = data.withColumn('forcast_id', concat(lit("fcast"), lit(datetime.now().strftime("%m%d%Y%H%M"))))
dff.show()
dff.write.parquet(f'/home/aravind/apache_airflow/transform_data/forcast/forcast_{time_now}.parquet')
