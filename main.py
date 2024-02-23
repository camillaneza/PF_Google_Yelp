# Importaciones
from fastapi import FastAPI, Path, HTTPException
from fastapi.responses import HTMLResponse
import asyncio
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
import pyarrow.parquet as pq
import os

from fastapi import FastAPI
from typing import List
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity



####################################### CARGA DE DATOS ##########################################
from fastapi import FastAPI
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

app = FastAPI()

# Cargar los datos necesarios (asegúrate de que 'matrix' esté definido previamente)
matrix = pd.read_parquet('data/muestra.parquet') 

# Calcular la similitud coseno entre los usuarios
user_similarity_cosine = pd.DataFrame(cosine_similarity(matrix.fillna(0)), index=matrix.index, columns=matrix.index)

# Mostrar la matriz de similitud
print(user_similarity_cosine)

@app.get("/matriz-similitud")
async def mostrar_matriz_similitud():
    return user_similarity_cosine.to_dict()

@app.get("/recomendar/{user_id}")
async def recomendar(user_id: int):
# Definir el umbral de similitud
    user_similarity_threshold = 0.3
    
    # Calcular la similitud coseno entre los usuarios
    user_similarity_cosine = pd.DataFrame(cosine_similarity(matrix.fillna(0)), index=matrix.index, columns=matrix.index)
    
    # Verificar si el ID de usuario existe en la matriz de similitud
    if user_id not in user_similarity_cosine.index:
        return {"message": "El ID de usuario no existe en la base de datos."}
    
    # Obtener los usuarios similares para el usuario dado
    similar_users = user_similarity_cosine.loc[user_id][user_similarity_cosine.loc[user_id] > user_similarity_threshold].sort_values(ascending=False)
    
    # Obtener las calificaciones de los usuarios similares
    similar_user_ratings = matrix.loc[similar_users.index]
    similar_user_ratings = similar_user_ratings.drop(user_id, axis=0).dropna(axis=1, how='all')
    
    # Calcular las recomendaciones
    item_score = {}
    for business_id, business_ratings in similar_user_ratings.items():
        total_score = 0
        total_similarity = 0
        for similar_user_id, similarity_score in similar_users.items():
            rating = business_ratings.get(similar_user_id)
            if not pd.isna(rating):
                total_score += similarity_score * rating
                total_similarity += similarity_score
        if total_similarity > 0:
            item_score[business_id] = total_score / total_similarity
    
    # Convertir el diccionario en un DataFrame y ordenarlo por puntaje
    item_score_df = pd.DataFrame(item_score.items(), columns=['business_id', 'score'])
    ranked_recommendations = item_score_df.sort_values(by='score', ascending=False)
    
    # Seleccionar las top m recomendaciones
    m = 10
    top_recommendations = ranked_recommendations.head(m)
    
    # Devolver las recomendaciones como lista de diccionarios
    recommendations = top_recommendations.to_dict(orient='records')
    
    return recommendations





def presentacion():
    '''
    Genera una página de presentación HTML para la API Steam de consultas de videojuegos.
    
    Returns:
    str: Código HTML que muestra la página de presentación.
    '''
    return '''
        <html>
            <head>
                <title>API Steam</title>
                <style>
                    body {
                        color: white; 
                        background-color: black; 
                        font-family: Arial, sans-serif;
                        padding: 20px;
                    }
                    h1 {
                        color: white;
                        text-align: center;
                    }
                    p {
                        color: white;
                        text-align: center;
                        font-size: 18px;
                        margin-top: 20px;
                    }
                    footer {
                        text-align: center;
                    }
                </style>
            </head>
            <body>
                <h1>API de consultas de Data Insight Solutions</h1>
                <p>Bienvenido a la API de consultas para clientes (v. Beta).</p>
                
                <p>INSTRUCCIONES:</p>
                <p>Escriba <span style="background-color: lightgray;">/docs</span> a continuación de la URL actual de esta página para interactuar con la API</p>
                
            </body>
        </html>
    '''

    # Página de inicio
@app.get(path="/", response_class=HTMLResponse, tags=["Home"])
def home():
    return presentacion()
