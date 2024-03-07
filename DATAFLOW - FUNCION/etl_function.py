from google.cloud.storage import Blob
from google.cloud import storage
from google.cloud import bigquery
import string as string
import pandas as pd
import gcsfs
from io import BytesIO
import db_dtypes
import json

def etl(event, context):

    file = event
    source_file_name=file['name']

    print(event)

    source_bucket_name = file['bucket']
    print(f"Uploaded archivo {source_file_name} al bucket {source_bucket_name}.")

    client = storage.Client(project="Proyecto-final-henry")
    destination_bucket_name = 'df-yelp-limpios'
    destination_file_name=file['name']

    #Buckets
    source_bucket = client.get_bucket(source_bucket_name)
    destination_bucket = client.get_bucket(destination_bucket_name)

    #Objeto
    source_blob = source_bucket.get_blob(source_file_name)

    # Crea un objeto blob para el archivo de destino
    destination_blob = destination_bucket.blob(destination_file_name)
    blop = source_bucket.blob(source_file_name)
    data = blop.download_as_string()

    #FILTRADO Y LIMPIEZA DE BUSINESS

    if "Yelp/business" in source_file_name:
        print("Comenzando el procesamiento del archivo...")
        try:
            # Intentar leer el archivo json como si no tuviera saltos de linea
            business = pd.read_json(BytesIO(data))
        except ValueError as e:
            if 'Trailing data' in str(e):
                # Leer el archivo json conteniendo saltos de linea
                business = pd.read_json(BytesIO(data), lines = True)
            else:
                # Cualquier otro error
                print('Ocurrió un error cargando el archivo JSON:', e)

        print("Se creo el dataframe")

        #Se imprime la dimension inicial del dataframe
        print(business.shape)

        #Se guardan en una lista los estados seleccionados
        estados = ['GA', 'FL', 'AL', 'NC', 'SC']

        #Se filtran los negocios seleccionando unicamente aquellos que se encuentran en los negocios seleccionados
        business=business[business['state'].isin(estados)]

        #Se observan las dimensiones del nuevo dataset de business filtrado a traves de los estados
        print(f'Se hace el filtrado seleccionando unicamente estados, dimesion nueva: {business.shape}')

        #Se prosigue a filtrar el dataset seleccionando unicamente los negocios que pertenezcan a la categoria restaurantes
        business['categories'] = business['categories'].fillna('')
        print('Se hace limpieza de valores nulos de la variable categories')
        # Se seleccionan registros donde la variable "categories" contiene la palabra 'Restaurants'
        business = business[business['categories'].str.lower().str.strip().str.contains('restaurants')]
        print(f'Se hace el filtrado seleccionando unicamente restaurantes, dimesion nueva: {business.shape}')
        #Se procede a eliminar las columnas que no serán utilizadas en el modelo
        business.drop(columns=['address','postal_code','review_count','is_open','attributes','hours'],inplace=True)
        print(f'Se eliminan las columnas que no seran utilizadas en el modelo, columnas restantes: {business.columns}')
        print(f'Se ha hecho limpieza correctamente, la dimension final dell archivo business es de {business.shape}')

        print(business.info())
        #Guardemos el dataset modificado
        source_file_name = source_file_name.replace('.json', '.parquet')
        business.to_parquet(r'gs://' + destination_bucket_name + '/' + source_file_name)

        print(f"Archivo {source_file_name} del bucket {source_bucket_name} procesado con éxito.")
    #FILTRADO Y LIMPIEZA DE TIPS
    elif "Yelp/tips" in source_file_name:
        print("Comenzando el procesamiento del archivo...")
        try:
            # Intentar leer el archivo json como si no tuviera saltos de linea
            tips = pd.read_json(BytesIO(data))
        except ValueError as e:
            if 'Trailing data' in str(e):
                # Leer el archivo json conteniendo saltos de linea
                tips = pd.read_json(BytesIO(data), lines = True)
            else:
                # Cualquier otro error
                print('Ocurrió un error cargando el archivo JSON:', e)
        print(f"Se creo el dataframe de tips, dimension {tips.shape}")

        #Se importa el nuevo dataset de business
        business=pd.read_parquet('gs://pf_cleaned_data/Yelp/business.parquet')
        print(f'Se importo el dataset limpio de business, dimension {business.shape}')
        #Se guardan los ids unicos en una variable
        ids_business=business['business_id'].unique()
        print('Se guardan los ids unicos de los negocios')
        #Se filtra el dataset de tips
        tips= tips[tips['business_id'].isin(ids_business)]
        print(f'Se filtra el dataset de tips y queda un nuevo dataset con dimension {tips.shape}')
        #Se imprime la informacion de tips
        print(tips.info())
        #Guardemos el dataset modificado
        source_file_name = source_file_name.replace('.json', '.parquet')
        tips.to_parquet(r'gs://' + destination_bucket_name + '/' + source_file_name)

        print(f"Archivo {source_file_name} del bucket {source_bucket_name} procesado con éxito.")

    #FILTRADO Y LIMPIEZA DE CHECKIN
    elif "Yelp/checkin" in source_file_name:
        print("Comenzando el procesamiento del archivo...")
        try:
            # Intentar leer el archivo json como si no tuviera saltos de linea
            checkin = pd.read_json(BytesIO(data))
        except ValueError as e:
            if 'Trailing data' in str(e):
                # Leer el archivo json conteniendo saltos de linea
                checkin = pd.read_json(BytesIO(data), lines = True)
            else:
                # Cualquier otro error
                print('Ocurrió un error cargando el archivo JSON:', e)
        print(f"Se creo el dataframe de checkin, dimension {checkin.shape}")

        #Se importa el nuevo dataset de business
        business=pd.read_parquet('gs://pf_cleaned_data/Yelp/business.parquet')
        print(f'Se importo el dataset limpio de business, dimension {business.shape}')
        #Se guardan los ids unicos en una variable
        ids_business=business['business_id'].unique()
        print('Se guardan los ids unicos de los negocios')
        #Guardemos el dataset modificado
        checkin= checkin[checkin['business_id'].isin(ids_business)]
        print(f'Se filtra el dataset de checkin y queda un nuevo dataset con dimension {checkin.shape}')
        #Se imprime la informacion de checkin
        print(checkin.info())
        #Guardemos el dataset modificado
        source_file_name = source_file_name.replace('.json', '.parquet')
        checkin.to_parquet(r'gs://' + destination_bucket_name + '/' + source_file_name)

        print(f"Archivo {source_file_name} del bucket {source_bucket_name} procesado con éxito.")
    #FILTRADO Y LIMPIEZA DE REVIEWS
    elif "Yelp/reviews" in source_file_name:
        print("Comenzando el procesamiento del archivo...")
        try:
            # Intentar leer el archivo json como si no tuviera saltos de linea
            reviews= pd.read_parquet(BytesIO(data))
        except ValueError as e:
            if 'Trailing data' in str(e):
                # Leer el archivo json conteniendo saltos de linea
                reviews= pd.read_parquet(BytesIO(data), memory_map=True ,lines = True)
            else:
                # Cualquier otro error
                print('Ocurrió un error cargando el archivo JSON:', e)
        print(f"Se creo el dataframe de reviews, dimension {reviews.shape}")


        #Se importa el nuevo dataset de business
        business=pd.read_parquet('gs://pf_cleaned_data/Yelp/business.parquet')
        print(f'Se importo el dataset limpio de business, dimension {business.shape}')
        #Se guardan los ids unicos en una variable
        ids_business=business['business_id'].unique()
        print('Se guardan los ids unicos de los negocios')
        #Guardemos el dataset modificado
        reviews= reviews[reviews['business_id'].isin(ids_business)]
        print(f'Se filtra el dataset de reviews y queda un nuevo dataset con dimension {reviews.shape}')
        print(reviews.info())
        reviews['date'] = pd.to_datetime(reviews['date']).dt.date
        #Guardemos el dataset modificado
        source_file_name = source_file_name.replace('.parquet', '.parquet')
        reviews.to_parquet(r'gs://' + destination_bucket_name + '/' + source_file_name)

        print(f"Archivo {source_file_name} del bucket {source_bucket_name} procesado con éxito.")

    elif "Yelp/users" in source_file_name:
        print("Comenzando el procesamiento del archivo...")
        try:
            # Intentar leer el archivo json como si no tuviera saltos de linea
            users= pd.read_parquet(BytesIO(data))
        except ValueError as e:
            if 'Trailing data' in str(e):
                # Leer el archivo json conteniendo saltos de linea
                users= pd.read_parquet(BytesIO(data), memory_map=True ,lines = True)
            else:
                # Cualquier otro error
                print('Ocurrió un error cargando el archivo parquet:', e)
        print(f"Se creo el dataframe de users, dimension {users.shape}")

        #Se importa la columna user_id del dataset limpio de reviews
        client = bigquery.Client(project="Proyecto-final-henry")
        sql_query = ('''SELECT *
                FROM Proyecto-final-henry.datawarehouse.reviews_yelp
                ''')
        reviews = client.query(sql_query).to_dataframe()
        print(f'Dataset de review cargado, dimension: {reviews_user_id.shape}')
        reviews_user_id = reviews['user_id'].unique()
        users = users[users['user_id'].isin(reviews_user_id)]

        print(f'Se filtra el dataset de reviews y queda un nuevo dataset con dimension {users.shape}')
        print(users.info())

        #Guardemos el dataset modificado
        source_file_name = source_file_name.replace('.parquet', '.parquet')
        users.to_parquet(r'gs://' + destination_bucket_name + '/' + source_file_name)
        print(f"Archivo {source_file_name} del bucket {source_bucket_name} procesado con éxito.")

    elif "Google/metadata_sitios" in source_file_name:
        print("Comenzando el procesamiento del archivo...")
        try:
            # Intentar leer el archivo json como si no tuviera saltos de linea
            sitios = pd.read_json(BytesIO(data))
        except ValueError as e:
            if 'Trailing data' in str(e):
                # Leer el archivo json conteniendo saltos de linea
                sitios = pd.read_json(BytesIO(data), lines = True)
            else:
                # Cualquier otro error
                print('Ocurrió un error cargando el archivo JSON:', e)

        print("Se creo el dataframe")

        #Borramos la columnas que no usamos
        sitios.drop(columns=['hours', 'state', 'relative_results','url'],inplace=True)

        #Convertir la lista de la columna Categoria
        sitios['category'] = sitios['category'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

        #Convertir la lista de la columna MISC
        sitios = sitios.map(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

        # Elimando Nulos
        sitios.dropna(subset=['category'], inplace=True)
        print(f" Shape {sitios.shape}")
        #Filtrio por Restaurant o que contenga Restaurant
        sitios = sitios[sitios['category'].str.contains('Restaurant', case=False)]

        #Creamos columnas City y State
        sitios['city'] = sitios['address'].str.split(',').str[-2].str.strip()
        sitios['state'] = sitios['address'].str.extract(r', (\w{2}) \d+')
        print(f" Shape {sitios.shape}")

        # Elimando duplicados
        sitios.drop_duplicates(inplace=True)
        print(f" Shape {sitios.shape}")

        #Filtro por estado PREGUNTAR ESTADOS
        #Se guardan en una lista los estados seleccionados
        estados = ['GA', 'FL']

        #Se filtran los estados
        sitios=sitios[sitios['state'].isin(estados)]

        print(f" Shape {sitios.shape}")

        #Guardemos el dataset filtrado
        source_file_name = source_file_name.replace('.json', '.parquet')
        sitios.to_parquet(r'gs://' + destination_bucket_name + '/' + source_file_name)

        print(f"Archivo {source_file_name} del bucket {source_bucket_name} procesado con éxito.")
    elif "Google/reviews_estados" in source_file_name:
        print("Comenzando el procesamiento del archivo...")
        try:
            # Intentar leer el archivo json como si no tuviera saltos de linea
            reviews_google = pd.read_json(BytesIO(data))
        except ValueError as e:
            if 'Trailing data' in str(e):
                # Leer el archivo json conteniendo saltos de linea
                reviews_google = pd.read_json(BytesIO(data), lines = True)
            else:
                # Cualquier otro error
                print('Ocurrió un error cargando el archivo JSON:', e)

        print("Se creo el dataframe")

        #Convertimos la columna time a date
        reviews_google["time"] = pd.to_datetime(reviews_google["time"], unit='ms').dt.date
        #reviews_google["time"] = pd.to_datetime(reviews_google['time'], format='%Y-%m-%d')
        #renombrar la columna time
        reviews_google.rename(columns={'time':'date'}, inplace=True)

        #Borrar columnas y duplicados
        reviews_google.drop(columns=['pics', 'resp', 'name'],inplace=True)
        reviews_google.drop_duplicates(inplace=True)

        #Creamos columna state
        if 'Georgia' in source_file_name:
            state = 'GA'
        elif 'Florida' in source_file_name:
            state = 'FL'
        else:
            print(f"No se encuentra en los estados para analizar. No se procesará el archivo")

        reviews_google['state'] = state

        print(f" Shape {reviews_google.shape}")

        #Guardemos el dataset filtrado
        source_file_name = source_file_name.replace('.json', '.parquet')
        reviews_google.to_parquet(r'gs://' + destination_bucket_name + '/' + source_file_name)

        print(f"Archivo {source_file_name} del bucket {source_bucket_name} procesado con éxito.")
    else:
        print("El archivo no corresponde a ninguna carpeta. No se procesará.")