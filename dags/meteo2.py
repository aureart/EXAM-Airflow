import json
import requests
from datetime import datetime
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import os


####Fonctions

def fetch_and_save_weather_data(*args, **kwargs):
    # Liste des villes stockées dans Airflow Variables
    cities = Variable.get("cities", deserialize_json=True)
    
    # Votre clé API (remplacez 'your_api_key' par votre véritable clé API)
    api_key = 'dca12b7a9f8656657ea14739436c167d'
    # Loop to retrieve the data of the diffrent cities
    json_list = []
    for city in cities:
        # Construction de l'URL pour la requête API
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
        
        # Faire la requête à l'API
        response = requests.get(url)
        if response.status_code == 200:
            # Charger les données météorologiques
            data = response.json()
            
            # Construire un nom de fichier avec la date et l'heure actuelles
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
            filename = f"{timestamp}.json"
                   
            #ajout du JSON object dans la list (pour chaque ville)
            json_list.append(data)      
            # Enregistrer les données dans un fichier dans le dossier /app/raw_files
            with open(f"/app/raw_files/{filename}", 'w') as f:
                json.dump(json_list, f)
        else:
            print(f"Failed to fetch data for city {city}")

# code donné pour  task 2 et 3 transformation des données
def transform_data_into_csv(n_files=None, filename='data.csv'):
    parent_folder = '/app/raw_files'
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
        #print(data_city)  # Ajoutez ceci pour déboguer
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )
    df = pd.DataFrame(dfs)

    print('\n', df.head(10))

    df.to_csv(os.path.join('/app/clean_data', filename), index=False)

'''
    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data = json.load(file)  # Charger le fichier JSON
            # Extraire les données nécessaires
            temperature = data['main']['temp']
            city = data['name']
            pressure = data['main']['pressure']
            # Convertir le timestamp UNIX (dt) en format lisible
            date = pd.to_datetime(data['dt'], unit='s')
            # Ajouter un dictionnaire avec les données extraites à la liste dfs
            dfs.append({
                'temperature': temperature,
                'city': city,
                'pressure': pressure,
                'date': date
            }
            )
'''



def transform_last_20_to_csv():
    transform_data_into_csv(n_files=20, filename='data.csv')

def transform_all_to_csv():
    transform_data_into_csv(n_files=None, filename='fulldata.csv')

### creation du dag
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 2),   #bonne pratique, mettre une date
    'retries': 1,
}
dag = DAG(
    dag_id='my_exam_dagv3',
    tags = ['exam'],
    default_args=default_args,
    description='A DAG to retrieve meteo',
    schedule_interval='* * * * *',
    catchup=False

)


#### creation des taches
task1 = PythonOperator(
    task_id='fetch_and_save_weather_data',
    python_callable=fetch_and_save_weather_data,
    dag=dag,
)
task2 = PythonOperator(
    task_id='transform_last_20_to_csv',
    python_callable=transform_last_20_to_csv,
    dag=dag,
)

task3 = PythonOperator(
    task_id='transform_all_to_csv',
    python_callable=transform_all_to_csv,
    dag=dag,
)


###ordonnancement des tasks
task1 >> task2
task1 >> task3
