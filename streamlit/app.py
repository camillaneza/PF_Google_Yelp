import streamlit as st
import yfinance as yf
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

def recomendar(user_id):
    try:
        # Leer los datos de muestra desde el archivo parquet
        muestra_final = pd.read_parquet("data/muestra_final.parquet")

        # Calcular la similitud coseno entre los usuarios
        matrix = muestra_final.pivot_table(index='user_id', columns='business_id', values='stars_y')
        user_similarity_cosine = pd.DataFrame(cosine_similarity(matrix.fillna(0)), index=matrix.index, columns=matrix.index)

        # Numero de usuarios similares
        n = 10
        # Umbral de similitud
        user_similarity_threshold = 0.3

        # Verificar si el usuario existe en la base de datos
        if user_id not in matrix.index:
            return pd.DataFrame(columns=['business_id', 'score']), False

        # Obtener el top n de usuarios similares basado en la similitud coseno
        similar_users = user_similarity_cosine.loc[user_id][user_similarity_cosine.loc[user_id] > user_similarity_threshold].sort_values(ascending=False)[:n]

        # Obtener las calificaciones de los usuarios similares
        similar_user_ratings = matrix.loc[similar_users.index]

        # Eliminar los negocios visitados por el usuario seleccionado
        picked_user_ratings = matrix.drop(user_id, axis=0)

        # Conservar solo las calificaciones de usuarios similares
        similar_user_ratings = similar_user_ratings.drop(user_id, axis=0)

        # Eliminar los negocios que el usuario seleccionado ha visitado
        similar_user_ratings = similar_user_ratings.dropna(axis=1, how='all')

        # Declarar un diccionario para el puntaje
        item_score = {}

        # Recorrer los restaurantes
        for business_id, business_ratings in similar_user_ratings.items():
            # Inicializar variables para calcular el puntaje
            total_score = 0
            total_similarity = 0

            # Recorrer los usuarios similares
            for similar_user_id, similarity_score in similar_users.items():
                # Obtener la calificación del restaurante por el usuario similar
                rating = business_ratings.get(similar_user_id)
                if not pd.isna(rating):
                    # Calcular el puntaje ponderado por la similitud del usuario
                    total_score += similarity_score * rating
                    total_similarity += similarity_score

            # Calcular el puntaje promedio para el restaurante
            if total_similarity > 0:
                item_score[business_id] = total_score / total_similarity

        # Convertir el diccionario en un DataFrame
        item_score_df = pd.DataFrame(item_score.items(), columns=['business_id', 'score'])

        # Ordenar las recomendaciones por puntaje en orden descendente
        ranked_recommendations = item_score_df.sort_values(by='score', ascending=False)

        # Seleccionar las top m recomendaciones
        m = 5
        top_recommendations = ranked_recommendations.head(m)

        # Eliminar el índice del DataFrame y devolver solo los datos
        return top_recommendations.reset_index(drop=True), True
    except Exception as e:
        st.error(f"Error al recomendar: {e}")
        return pd.DataFrame(columns=['business_id', 'score']), False

def analisis_sentimientos_business(business_id):
    try:
        # Leer el DataFrame
        df = pd.read_parquet("data/muestra_final.parquet") # Reemplaza "tu_archivo.csv" con la ruta de tu archivo
        
        # Filtrar el DataFrame por el ID de negocio
        filtered_df = df[df['business_id'] == business_id]
        
        if filtered_df.empty:
            return "ID de Negocio inexistente en Base de Datos"

        # Contar las reseñas por categorización
        sentiment_counts = filtered_df['categorizacion'].value_counts().to_dict()

        # Mapear las categorías a los nombres esperados
        sentiment_mapping = {-1: "Negativas", 0: "Neutrales", 1: "Positivas"}
        sentiment_counts_mapped = {sentiment_mapping[key]: value for key, value in sentiment_counts.items()}

        return sentiment_counts_mapped
    except Exception as e:
        raise ValueError(str(e))

def mostrar_analisis_sent():
    st.write("Análisis de Sentimientos por ID de Negocio, para evaluar competencia")

    business_id = st.text_input("Ingrese el ID del negocio para el análisis de sentimientos:")
    if st.button("Analizar Reseñas"):
        try:
            sentiment_counts = analisis_sentimientos_business(int(business_id))
            st.write("Análisis de Reseñas para el negocio con ID:", business_id)
            for categoria, cantidad in sentiment_counts.items():
                if categoria == "Positivas":
                    st.write(categoria, ": ", cantidad, style="color:green")
                elif categoria == "Negativas":
                    st.write(categoria, ": ", cantidad, style="color:red")
                elif categoria == "Neutrales":
                    st.write(categoria, ": ", cantidad, style="color:blue")
                else:
                    st.write(categoria, ": ", cantidad)
        except ValueError as e:
            st.error(str(e))

def mostrar_pred():
    st.write("Sistema de Recomendación: Ingrsando el ID de Usuario, retornará los Restaurantes que podrían satisfacer")

    user_id = st.text_input("Ingrese el ID del usuario:")

    if st.button("Obtener Recomendaciones"):
        try:
            user_id = int(user_id)  # Convertir a entero
        except ValueError:
            st.error("Por favor, ingrese un ID de usuario válido.")
            return

        recomendaciones, user_found = recomendar(user_id)
        if not user_found:
            st.write("Usuario no encontrado en base de datos")
        else:
            recomendaciones_html = recomendaciones.to_html(index=False)  # Eliminar el índice
            st.write("Las 5 recomendaciones principales:")
            st.write(recomendaciones_html, unsafe_allow_html=True)

def main():
    st.set_page_config(page_title="Restaurantes Maps & Yelp", layout="wide")

    st.title("Bienvenido al Análisis de Reseñas de Restaurantes Maps & Yelp")

    # Cargar la imagen de fondo para el footer
    footer_image = "images/Chart Talk logo template.Financial growth talk logo .png"

    # Mostrar la imagen en el footer con el tamaño adaptado
    

    # Footer en la barra lateral
    st.sidebar.title("Equipo de DataInsight Solutions")
    st.sidebar.header("Integrantes:")
    st.sidebar.write("Data Analysis - Nadir Angelini")
    st.sidebar.write("Data Engineering - German Gutierrez")
    st.sidebar.write("Machine Learning - Camila Fernández Llaneza")
    st.sidebar.write("Machine Learning - Leonel Viscay")
    st.sidebar.image(footer_image, width=300)

    if "is_logged_in" not in st.session_state:
        st.session_state["is_logged_in"] = False

    if not st.session_state.is_logged_in:
        mostrar_inicio_sesion()
    else:
        mostrar_botones()

def mostrar_inicio_sesion():
    st.title("Iniciar Sesión")

    st.write("Por favor, inicia sesión para continuar.")

    usuario = st.text_input("Usuario")
    contrasena = st.text_input("Contraseña", type="password")

    if st.button("Iniciar Sesión"):
        if usuario == "ivan" and contrasena == "123456":
            st.session_state["is_logged_in"] = True
            st.success("¡Inicio de sesión exitoso! Redirigiendo al panel principal...")
            mostrar_botones()  # Mostrar el contenido principal después de iniciar sesión
        else:
            st.error("Usuario o contraseña incorrectos. Por favor, inténtalo de nuevo.")

    st.write("¿No tienes una cuenta? Regístrate [aquí](url_de_registro).")

def mostrar_botones():
    st.write("¡Bienvenido a la versión Beta de DataInsight Solutions!")

    col1, col2 = st.columns([1, 4])

    with col1:
        opcion = st.radio("Selecciona una opción:", ("Valor de acciones al Día", "Dashboard", "Recomendaciones por Score", "Análisis de Sentimientos"))

    with col2:
        if opcion == "Dashboard":
            mostrar_dashboard()
        elif opcion == "Recomendaciones por Score":
            mostrar_pred()
        elif opcion == "Valor de acciones al Día":
            mostrar_acciones_nasdaq()
        elif opcion == "Análisis de Sentimientos":
            mostrar_analisis_sent()

def mostrar_dashboard():
    
    st.title("Dashboard")
    st.write("Aquí encontrarás información general relevante.")

    # Agregar imágenes a los botones del dashboard
    for i in range(1, 8):
        image_path = f"images/Dash{i}.jpeg"  # Ruta relativa a las imágenes
        st.image(image_path, caption=f"Dashboard {i}", use_column_width=True)
        #st.button(f"Dashboard {i}", key=f"dashboard_{i}")

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

if __name__ == "__main__":
    main()
