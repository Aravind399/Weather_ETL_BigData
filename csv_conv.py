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

# Now you can use 'sc' for RDD operations or use 'spark' for DataFrame operations


data = spark.read.json('/home/aravind/apache_airflow/raw_weather_data/',multiLine=True)


df_current = data.selectExpr(
    #"current.air_quality.*",
    "current.cloud",
    "current.condition.*",
    "current.feelslike_c",
    "current.feelslike_f",
    "current.gust_kph",
    "current.gust_mph",
    "current.humidity",
    "current.is_day",
    "current.last_updated",
    "current.last_updated_epoch",
    "current.precip_in",
    "current.precip_mm",
    "current.pressure_in",
    "current.pressure_mb",
    "current.temp_c",
    "current.temp_f",
    "current.uv",
    "current.vis_km",
    "current.vis_miles",
    "current.wind_degree",
    "current.wind_dir",
    "current.wind_kph",
    "current.wind_mph"
)

# Print the schema of the flattened DataFrame
df_current.printSchema()



time_now  = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M') 

df_current.write.parquet(f'/home/aravind/apache_airflow/current_data/current_{time_now}.parquet')




forecast_df = data.selectExpr(
    "explode(forecast.forecastday) as forecastday",
    "current.*"
).selectExpr(
    "forecastday.date as date",
    "forecastday.date_epoch as date_epoch",
    "forecastday.day.avghumidity as avghumidity",
    "forecastday.day.avgtemp_c as avgtemp_c",
    "forecastday.day.avgtemp_f as avgtemp_f",
    "forecastday.day.avgvis_km as avgvis_km",
    "forecastday.day.avgvis_miles as avgvis_miles",
    "forecastday.day.condition.*",
    "forecastday.day.daily_chance_of_rain as daily_chance_of_rain",
    "forecastday.day.daily_chance_of_snow as daily_chance_of_snow",
    "forecastday.day.daily_will_it_rain as daily_will_it_rain",
    "forecastday.day.daily_will_it_snow as daily_will_it_snow",
    "forecastday.day.maxtemp_c as maxtemp_c",
    "forecastday.day.maxtemp_f as maxtemp_f",
    "forecastday.day.maxwind_kph as maxwind_kph",
    "forecastday.day.maxwind_mph as maxwind_mph",
    "forecastday.day.mintemp_c as mintemp_c",
    "forecastday.day.mintemp_f as mintemp_f",
    "forecastday.day.totalprecip_in as totalprecip_in",
    "forecastday.day.totalprecip_mm as totalprecip_mm",
    "forecastday.day.totalsnow_cm as totalsnow_cm",
    "forecastday.day.uv as uv"
)

# Show the flattened forecast DataFrame
forecast_df.show()
forecast_df.printSchema()


time_now  = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M') 

forecast_df.write.parquet(f'/home/aravind/apache_airflow/forcast_data/forecast_{time_now}.parquet')

location_df = data.select('location.*')

location_df.write.parquet(f'/home/aravind/apache_airflow/location_data/location_{time_now}.parquet')






