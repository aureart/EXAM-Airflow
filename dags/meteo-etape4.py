import json
import requests
from datetime import datetime
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import os

from sklearn.model_selection import cross_val_score
#from sklearn.model_selection import train_test_split

from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor

from joblib import dump




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


def transform_last_20_to_csv():
    transform_data_into_csv(n_files=20, filename='data.csv')

def transform_all_to_csv():
    transform_data_into_csv(n_files=None, filename='fulldata.csv')

def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()
    return model_score


def train_and_save_model(model, X, y, path_to_model='./app/model.pckl'):
    # training the model
    model.fit(X, y)
    # saving model
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)

#etape4
    #je rajoute une etape 4 pour ne pas avoir a preparer les features et target a chaque model.
    # je choisis de les stocker dans /clean_data sous forme de fichier
    # j'aurais pu en faire des variables

def prepare_and_store_data(*args, **kwargs):
    prepare_data('/app/clean_data/fulldata.csv')


#etape 4'
def evaluate_model_lr(task_instance):    
    #X, y = prepare_data('./clean_data/fulldata.csv')
    path_to_X='/app/clean_data/features.csv'
    path_to_y='/app/clean_data/target.csv'
    X = pd.read_csv(path_to_X)
    y = pd.read_csv(path_to_y)
    model = LinearRegression()
   # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2)
   # model.fit(X, y)
    score_lr = compute_model_score(model, X, y)
    task_instance.xcom_push( 
        key="score_lr", 
        value=score_lr
        )

#etape 4''
def evaluate_model_dt(task_instance):
   #X, y = prepare_data('/app/clean_data/fulldata.csv')
    path_to_X='/app/clean_data/features.csv'
    path_to_y='/app/clean_data/target.csv'
    X = pd.read_csv(path_to_X)
    y = pd.read_csv(path_to_y)
    model = DecisionTreeRegressor()
    #model.fit(X, y)
    score_dt = compute_model_score(model, X, y)
    task_instance.xcom_push( 
        key="score_dt", 
        value=score_dt
        )



#etape 4'''
def evaluate_model_rf(task_instance):    
    #X, y = prepare_data('./clean_data/fulldata.csv')
    path_to_X='/app/clean_data/features.csv'
    path_to_y='/app/clean_data/target.csv'
    X = pd.read_csv(path_to_X)
    y = pd.read_csv(path_to_y)
    model = RandomForestRegressor()
    score_rf = compute_model_score(model, X, y)
    task_instance.xcom_push( 
        key="score_rf", 
        value=score_rf
        )

#etape5
def select_and_save_model(task_instance):
    #X, y = prepare_data('./clean_data/fulldata.csv')
    path_to_X='/app/clean_data/features.csv'
    path_to_y='/app/clean_data/target.csv'
    X = pd.read_csv(path_to_X)
    y = pd.read_csv(path_to_y)
    scores = {
        'LinearRegression': task_instance.xcom_pull(task_ids='evaluate_lr', key=f'score_lr'),
        'DecisionTreeRegressor': task_instance.xcom_pull(task_ids='evaluate_dt', key=f'score_dt'),
        'RandomForestRegressor': task_instance.xcom_pull(task_ids='evaluate_rf', key=f'score_rf')
    }
    # Sélectionner le meilleur modèle
    best_model_name = max(scores, key=scores.get)
    
    # Instancier et entraîner le meilleur modèle
    if best_model_name == 'LinearRegression':
        best_model = LinearRegression()
    elif best_model_name == 'DecisionTreeRegressor':
        best_model = DecisionTreeRegressor()
    elif best_model_name == 'RandomForestRegressor':
        best_model = RandomForestRegressor()

    train_and_save_model(best_model,X,y,'/app/clean_data/best_model.pickle')
    #score_lr = task_instance.xcom_pull(key="score_lr", task_ids=['evaluate_lr'])
    #score_dt = task_instance.xcom_pull(key="score_dt", task_ids=['evaluate_dt'])
    #score_rf = task_instance.xcom_pull(key="score_rf", task_ids=['evaluate_rf'])



def prepare_data(path_to_data='/app/clean_data/fulldata.csv'):
    # reading data
    df = pd.read_csv(path_to_data)
    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c]

        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)
                        ] = df_temp['temperature'].shift(-i)

        # deleting null values
        df_temp = df_temp.dropna()

        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(
        dfs,
        axis=0,
        ignore_index=False
    )

    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)

    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    #Placer X et y dans /app/clean_data/
    features.to_csv(os.path.join('/app/clean_data', 'features.csv'), index=False)
    target.to_csv(os.path.join('/app/clean_data', 'target.csv'), index=False)

    return features, target


if __name__ == '__main__':

    #X, y = prepare_data('./clean_data/fulldata.csv')

    score_lr = compute_model_score(LinearRegression(), X, y)
    score_dt = compute_model_score(DecisionTreeRegressor(), X, y)
    score_rf = compute_model_score(RandomForestRegressor(), X, y)
    
    # using neg_mean_square_error
    if score_lr < score_dt:
        train_and_save_model(
            LinearRegression(),
            X,
            y,
            '/app/clean_data/best_model.pickle'
        )
    else:
        train_and_save_model(
            DecisionTreeRegressor(),
            X,
            y,
            '/app/clean_data/best_model.pickle'
        )

        
### creation du dag
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 2),   #bonne pratique, mettre une date
    'retries': 1,
}
dag = DAG(
    dag_id='my_exam_dag_etape4',
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

task4 = PythonOperator(
    task_id='prepare_and_store_data',
    python_callable=prepare_and_store_data,
    dag=dag,
)
evaluate_lr = PythonOperator(
    task_id='evaluate_lr',
    python_callable=evaluate_model_lr,
    op_kwargs={'model_name': 'LinearRegression'},
    dag=dag,
)

evaluate_dt = PythonOperator(
    task_id='evaluate_dt',
    python_callable=evaluate_model_dt,
    op_kwargs={'model_name': 'DecisionTreeRegressor'},
    dag=dag,
)

evaluate_rf = PythonOperator(
    task_id='evaluate_rf',
    python_callable=evaluate_model_rf,
    op_kwargs={'model_name': 'RandomForestRegressor'},
    dag=dag,
)

task5 = PythonOperator(
    task_id='select_and_save_model',
    python_callable=select_and_save_model,
    dag=dag,
)


###ordonnancement des tasks
task1 >> task2 
task1 >> task3 >> task4 >> [evaluate_lr, evaluate_dt, evaluate_rf] >> task5
