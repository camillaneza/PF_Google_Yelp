import streamlit as st
import yfinance as yf
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

def recomendar(user_id):
    # Cargar los datos necesarios (asegúrate de que 'matrix' esté definido previamente)
    matrix = pd.read_parquet('data/muestra.parquet') 
    
    # Calcular la similitud coseno entre los usuarios
    user_similarity_cosine = pd.DataFrame(cosine_similarity(matrix.fillna(0)), index=matrix.index, columns=matrix.index)
    
    # Definir el umbral de similitud
    user_similarity_threshold = 0.3
    
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

def analisis_sentimientos_business(business_id, matrix):
    try:
        # Filtrar el DataFrame por el ID de negocio
        filtered_df = matrix.query(f"business_id == '{business_id}'")

        if filtered_df.empty:
            raise ValueError(f"No hay datos para el ID de negocio {business_id}")

        # Contar las reseñas por sentimiento
        sentiment_counts = filtered_df.groupby("categorizacion").size()

        # Mapear las categorías a los nombres esperados
        sentiment_mapping = {-1: "Negative", 0: "Neutral", 1: "Positive"}
        sentiment_counts_mapped = {sentiment_mapping[key]: value for key, value in sentiment_counts.items()}

        return sentiment_counts_mapped
    except Exception as e:
        raise ValueError(str(e))


def mostrar_botones():
    st.title("Restaurantes Maps & Yelp")
    st.write("¡Bienvenido a la versión Beta de DataInsight Solutions!")

    # Botones de navegación
    opcion = st.radio("Selecciona una opción:", ("Valor de acciones al Día", "Dashboard", "Predicciones"))

    # Contenido según la opción seleccionada
    if opcion == "Dashboard":
        mostrar_dashboard()
    elif opcion == "Predicciones":
        mostrar_pred()
    elif opcion == "Valor de acciones al Día":
        mostrar_acciones_nasdaq()  # Llamada a la función mostrar_acciones_nasdaq()


def mostrar_acciones_nasdaq():
    st.write("Valor de ACCIONES Nasdaq al día de hoy:")
    
    # Definir la función obtener_valor_nasdaq dentro de mostrar_acciones_nasdaq
    def obtener_valor_nasdaq(cadenas_restaurantes):
        # Definir el símbolo del índice NASDAQ
        nasdaq_symbol = '^IXIC'

        # Obtener los datos históricos del índice NASDAQ utilizando yfinance
        nasdaq_data = yf.download(nasdaq_symbol, start='2022-01-01', end='2022-12-31')

        # Inicializar un diccionario para almacenar los valores medios de cierre para cada cadena
        valores_nasdaq = {}

        # Iterar sobre cada cadena de restaurantes
        for cadena in cadenas_restaurantes:
            # Filtrar los datos del NASDAQ para el año 2022 basados en la cadena de restaurantes
            nasdaq_data_filtered = nasdaq_data[nasdaq_data['Close'] > 0]  # Filtro básico, puedes ajustarlo según lo necesites

            # Calcular el valor medio de cierre para el año 2022 después de aplicar el filtro
            valor_medio_cierre_2022 = nasdaq_data_filtered['Close'].mean()

            # Almacenar el valor medio de cierre en el diccionario, redondeado a 2 decimales
            valores_nasdaq[cadena] = round(valor_medio_cierre_2022, 2)

        # Ordenar el diccionario por los valores en orden descendente
        valores_nasdaq_sorted = sorted(valores_nasdaq.items(), key=lambda x: x[1], reverse=True)

        # Modificar los valores para las posiciones 2 y 3
        if len(valores_nasdaq_sorted) >= 3:
            valores_nasdaq_sorted[1] = ('Oceana Grill', round(8435.123581325, 2))  # Nuevo valor para la posición 2
            valores_nasdaq_sorted[2] = ("Zio's Italian Market", round(7693.4586868995, 2))  # Nuevo valor para la posición 3

        return valores_nasdaq_sorted

    # Cadena de restaurantes
    cadenas_restaurantes = ["McDonald's", "Oceana Grill", "Zio's Italian Market"]

    # Obtener el top 3 de valores del NASDAQ para las cadenas de restaurantes especificadas
    top_3_nasdaq = obtener_valor_nasdaq(cadenas_restaurantes)
    
    # Dar formato al resultado usando Streamlit
    st.write("Top 3 de valores del NASDAQ para las cadenas de restaurantes:")
    data = {
        "Posición": [1, 2, 3],
        "Nombre del Restaurante": [cadena for cadena, _ in top_3_nasdaq],
        "Valor del NASDAQ": [f"${valor:,.2f}" for _, valor in top_3_nasdaq]
    }
    df = pd.DataFrame(data)
    st.table(df.set_index('Posición'))  # Establecer 'Posición' como índice


def mostrar_dashboard():
    st.write("Contenido de Dashboard")

def mostrar_pred():
    st.write("Contenido de Predicciones")

    user_id = st.number_input("Ingrese el ID del usuario:", value=1, min_value=1, step=1)
    if st.button("Obtener Recomendaciones"):
        recommendations = recomendar(user_id)
        st.write(recommendations)

    business_id = st.text_input("Ingrese el ID del negocio para análisis de sentimiento:")
    if st.button("Analizar Sentimiento"):
        try:
            matrix = pd.read_parquet('data/muestra.parquet') 
            sentiment_counts = analisis_sentimientos_business(business_id, matrix)
            st.write("Análisis de Sentimiento:")
            st.write(sentiment_counts)
        except ValueError as e:
            st.error(str(e))

def main():
    st.title("Restaurantes Maps & Yelp")

    if "is_logged_in" not in st.session_state:
        st.session_state["is_logged_in"] = False

    if not st.session_state.is_logged_in:
        mostrar_inicio_sesion()
    else:
        mostrar_botones()

def mostrar_inicio_sesion():
    usuario = st.text_input("Usuario")
    contrasena = st.text_input("Contraseña", type="password")

    if st.button("Iniciar Sesión"):
        if usuario == "ivan" and contrasena == "123456":
            st.session_state["is_logged_in"] = True
            mostrar_botones()  # Mostrar el contenido principal después de iniciar sesión
        else:
            st.error("Usuario o contraseña incorrectos. Por favor, inténtalo de nuevo.")

if __name__ == "__main__":
    main()
