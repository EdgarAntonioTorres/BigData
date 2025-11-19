import json
import requests
import pandas as pd
import numpy as np
import sqlalchemy
import pymysql

def lambda_handler(event, context):
    """
    AWS Lambda function to collect weather data and store it in RDS
    """
    try:
        # API de OpenWeather
        OWM_key = "60fb44151ca0b23ebde5de0a2427c413"
        # Ciudad y país
        city = "Barcelona"
        country = "ES"
        # Configuración de AWS RDS
        schema = "gans"  # Nombre de tu base de datos
        host = "database-1.c3wi04qiuysy.us-east-2.rds.amazonaws.com"
        user = "admin"
        password = "gomaYjeff2705-"
        port = 3306
        # ===========================================================
        
        # Realizar solicitud a la API de OpenWeather
        response = requests.get(
            f'http://api.openweathermap.org/data/2.5/forecast/?q={city},{country}&appid={OWM_key}&units=metric&lang=en'
        )
        
        if response.status_code != 200:
            return {
                'statusCode': response.status_code,
                'body': json.dumps(f'Error al obtener datos de OpenWeather: {response.text}')
            }
        
        data = response.json()
        forecast_list = data.get('list', [])
        
        if not forecast_list:
            return {
                'statusCode': 404,
                'body': json.dumps('No se encontraron datos meteorológicos')
            }
        
        # Extraer información meteorológica
        times = []
        temperatures = []
        humidities = []
        weather_statuses = []
        wind_speeds = []
        rain_volumes = []
        snow_volumes = []
        
        for entry in forecast_list:
            times.append(entry.get('dt_txt', np.nan))
            temperatures.append(entry.get('main', {}).get('temp', np.nan))
            humidities.append(entry.get('main', {}).get('humidity', np.nan))
            weather_statuses.append(entry.get('weather', [{}])[0].get('main', np.nan))
            wind_speeds.append(entry.get('wind', {}).get('speed', np.nan))
            rain_volumes.append(entry.get('rain', {}).get('3h', np.nan))
            snow_volumes.append(entry.get('snow', {}).get('3h', np.nan))
        
        # Crear DataFrame
        df = pd.DataFrame({
            'weather_datetime': times,
            'temperature': temperatures,
            'humidity': humidities,
            'weather_status': weather_statuses,
            'wind': wind_speeds,
            'rain_qty': rain_volumes,
            'snow': snow_volumes,
            'municipality_iso_country': f"{city},{country}"
        })
        
        # Crear cadena de conexión
        con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
        
        # Probar la conexión
        engine = sqlalchemy.create_engine(con)
        connection = engine.connect()
        print("Conexión exitosa a AWS RDS")
        
        # Insertar datos en la base de datos
        df.to_sql('weather_data', if_exists='append', con=con, index=False)
        records_inserted = len(df)
        
        connection.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Datos meteorológicos insertados exitosamente',
                'records_inserted': records_inserted,
                'city': city,
                'country': country
            })
        }
        
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud HTTP: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error en la solicitud HTTP: {str(e)}')
        }
        
    except sqlalchemy.exc.SQLAlchemyError as e:
        print(f"Error de base de datos: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error de base de datos: {str(e)}')
        }
        
    except Exception as e:
        print(f"Error general: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error general: {str(e)}')
        }