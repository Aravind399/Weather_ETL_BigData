import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

import streamlit as st 
import pandas as pd
import matplotlib.pyplot as plt

get = st.sidebar.radio("select the options", options=("current", "forecast","reinforcement model"))
if get == 'forecast':
    df = pd.read_parquet('/home/aravind/apache_airflow/transform_data/forcast/')

    st.title('dataframe')
    st.write(df)


    # Sample data (assuming it's already loaded into a DataFrame named df)
    # df = ...

    # Convert 'date' column to datetime format
    df['date'] = pd.to_datetime(df['date'])

    # Sort DataFrame by 'date'
    df = df.sort_values(by='date')

    # Line Plot for Temperature Trends
    st.subheader('Average Temperature (°C) Over Time')
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['avgtemp_c'], marker='o', linestyle='-', label='Average Temperature (°C)')
    plt.xlabel('Date')
    plt.ylabel('Average Temperature (°C)')
    plt.legend()
    plt.xticks(rotation=45)
    st.pyplot(plt)

    # Bar Plot for Maximum and Minimum Temperature
    st.subheader('Maximum and Minimum Temperature (°C) Over Time')
    plt.figure(figsize=(12, 6))
    plt.bar(df['date'], df['maxtemp_c'], width=0.4, label='Max Temperature (°C)')
    plt.bar(df['date'], df['mintemp_c'], width=0.4, label='Min Temperature (°C)', alpha=0.7)
    plt.xlabel('Date')
    plt.ylabel('Temperature (°C)')
    plt.legend()
    plt.xticks(rotation=45)
    st.pyplot(plt)

    # Histogram for Humidity
    st.subheader('Distribution of Average Humidity')
    plt.figure(figsize=(10, 6))
    plt.hist(df['avghumidity'], bins=20, edgecolor='black')
    plt.xlabel('Average Humidity')
    plt.ylabel('Frequency')
    st.pyplot(plt)

    # Scatter Plot for Temperature vs. UV Index
    st.subheader('Average Temperature vs. UV Index')
    plt.figure(figsize=(10, 6))
    plt.scatter(df['avgtemp_c'], df['uv'], color='blue', alpha=0.6)
    plt.xlabel('Average Temperature (°C)')
    plt.ylabel('UV Index')
    st.pyplot(plt)

    # Bar Plot for Rain Chance
    st.subheader('Daily Chance of Rain')
    plt.figure(figsize=(12, 6))
    plt.bar(df['date'], df['daily_chance_of_rain'], color='green')
    plt.xlabel('Date')
    plt.ylabel('Chance of Rain (%)')
    plt.xticks(rotation=45)
    st.pyplot(plt)

    # Line Plot for Wind Speed
    st.subheader('Maximum Wind Speed (kph) Over Time')
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['maxwind_kph'], marker='o', linestyle='-', color='orange')
    plt.xlabel('Date')
    plt.ylabel('Maximum Wind Speed (kph)')
    plt.xticks(rotation=45)
    st.pyplot(plt)
elif get == 'current':

    dff = pd.read_parquet('/home/aravind/apache_airflow/archive_current_data/')
    st.title('current data')
    st.write(dff)

    dff['last_updated'] = pd.to_datetime(dff['last_updated'])

    # Sort DataFrame by 'last_updated'
    dff = dff.sort_values(by='last_updated')

    # Line Plot for Temperature Trends
    st.subheader('Temperature (°C) Over Time')
    plt.figure(figsize=(12, 6))
    plt.plot(dff['last_updated'], dff['temp_c'], marker='o', linestyle='-', color='blue', label='Temperature (°C)')
    plt.xlabel('Last Updated')
    plt.ylabel('Temperature (°C)')
    plt.xticks(rotation=45)
    st.pyplot(plt)

    # Line Plot for Humidity Trends
    st.subheader('Humidity Over Time')
    plt.figure(figsize=(12, 6))
    plt.plot(dff['last_updated'], dff['humidity'], marker='o', linestyle='-', color='green', label='Humidity (%)')
    plt.xlabel('Last Updated')
    plt.ylabel('Humidity (%)')
    plt.xticks(rotation=45)
    st.pyplot(plt)

    # Bar Plot for Pollutants
    pollutants = ['no2', 'o3', 'pm10', 'pm2_5']
    st.subheader('Air Pollutants Comparison')
    plt.figure(figsize=(12, 6))
    for pollutant in pollutants:
        plt.bar(dff['last_updated'], dff[pollutant], alpha=0.6, label=pollutant)
    plt.xlabel('Last Updated')
    plt.ylabel('Concentration')
    plt.legend()
    plt.xticks(rotation=45)
    st.pyplot(plt)

    # Line Plot for Pressure
    st.subheader('Atmospheric Pressure (mb) Over Time')
    plt.figure(figsize=(12, 6))
    plt.plot(dff['last_updated'], dff['pressure_mb'], marker='o', linestyle='-', color='purple', label='Pressure (mb)')
    plt.xlabel('Last Updated')
    plt.ylabel('Pressure (mb)')
    plt.xticks(rotation=45)
    st.pyplot(plt)

    # Line Plot for Wind Speed
    st.subheader('Wind Speed (kph) Over Time')
    plt.figure(figsize=(12, 6))
    plt.plot(dff['last_updated'], dff['wind_kph'], marker='o', linestyle='-', color='orange', label='Wind Speed (kph)')
    plt.xlabel('Last Updated')
    plt.ylabel('Wind Speed (kph)')
    plt.xticks(rotation=45)
    st.pyplot(plt)
