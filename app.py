import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Conectarse sin especificar una base de datos
engine = create_engine('mssql+pymssql://sa:SecurePass2024!@sqlserver:1433', isolation_level="AUTOCOMMIT")

with engine.connect() as conn:
    # Ejecutar consulta para verificar si la base de datos existe
    result = conn.execute(text("SELECT name FROM sys.databases WHERE name='lab06db'"))

    # Si la base de datos no existe, crearla
    if not result.fetchone():
        # Crear la base de datos sin transacción
        conn.execute(text("CREATE DATABASE lab06db"))
        print("Base de datos 'lab06db' creada.")
    else:
        print("La base de datos 'lab06db' ya existe.")

# Leer archivos de datos
def process_data_files():
    try:
        # 1. Leer 'u.genre' (GENERO)
        genre_df = pd.read_csv('./ml-100k/u.genre', sep='|', names=['genre_name', 'id'])
        print(f"Leídos {len(genre_df)} géneros")

        # 2. Leer 'u.occupation' (OCUPACION)
        occupation_df = pd.read_csv('./ml-100k/u.occupation', sep='|', names=['occupation_name'])
        print(f"Leídas {len(occupation_df)} ocupaciones")

        # 3. Leer 'u.user' (información demográfica)
        users_df = pd.read_csv('./ml-100k/u.user', sep='|', names=['user_id', 'age', 'gender', 'occupation', 'zip_code'])
        print(f"Leídos {len(users_df)} usuarios")

        # 4. Leer 'u.item' (información sobre películas)
        items_columns = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url'] + [f'genre_{i}' for i in
                                                                                                   range(19)]
        items_df = pd.read_csv('ml-100k/u.item', sep='|', names=items_columns, encoding='ISO-8859-1')
        print(f"Leídas {len(items_df)} películas")

        # Unir las columnas de géneros en una sola columna de "géneros"
        genres_columns = items_columns[5:]
        items_df['genres'] = items_df[genres_columns].apply(
            lambda row: ','.join([str(i) for i, val in enumerate(row) if val == 1]), axis=1)
        items_df = items_df[['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url', 'genres']]

        # 5. Leer 'u.data' (valoraciones)
        ratings_df = pd.read_csv('ml-100k/u.data', sep='\t', names=['user_id', 'movie_id', 'rating', 'timestamp'])
        ratings_df['timestamp'] = pd.to_datetime(ratings_df['timestamp'],
                                                 unit='s')  # Convertir la marca de tiempo a formato datetime
        print(f"Leídos {len(ratings_df)} valoraciones")

        # 6. Análisis de los datos
        analyze_data(users_df, items_df, ratings_df)

        # 7. Insertar en SQL Server
        insert_data_sql(users_df, items_df, ratings_df)

    except SQLAlchemyError as e:
        print(f"Error al conectar con la base de datos: {e}")
    except FileNotFoundError as e:
        print(f"Error de archivo: {e}")
    except Exception as e:
        print(f"Ocurrió un error: {e}")


# Función para realizar análisis descriptivo y gráficos
def analyze_data(users_df, items_df, ratings_df):
    # 1. Distribución de calificaciones
    rating_distribution = ratings_df['rating'].value_counts().sort_index()
    print("\nDistribución de calificaciones:")
    print(rating_distribution)

    # Gráfico de distribución de calificaciones
    plt.figure(figsize=(8, 6))
    sns.barplot(x=rating_distribution.index, y=rating_distribution.values, hue=rating_distribution.index,
                palette='viridis', legend=False)
    plt.title('Distribución de calificaciones')
    plt.xlabel('Calificación')
    plt.ylabel('Cantidad')
    plt.show()

    # 2. Promedio de calificaciones por película
    avg_ratings_per_movie = ratings_df.groupby('movie_id')['rating'].mean().sort_values(ascending=False)
    print("\nPromedio de calificaciones por película:")
    print(avg_ratings_per_movie.head())

    # Gráfico de promedio de calificaciones por película
    plt.figure(figsize=(8, 6))
    sns.histplot(avg_ratings_per_movie, kde=True, color='blue')
    plt.title('Distribución de Promedio de Calificaciones por Película')
    plt.xlabel('Promedio de Calificación')
    plt.ylabel('Frecuencia')
    plt.show()

    # 3. Distribución de usuarios por ocupación y género
    user_distribution_by_gender = users_df['gender'].value_counts()
    user_distribution_by_occupation = users_df['occupation'].value_counts()

    print("\nDistribución de usuarios por género:")
    print(user_distribution_by_gender)

    print("\nDistribución de usuarios por ocupación:")
    print(user_distribution_by_occupation)

    # Gráfico de distribución de género
    plt.figure(figsize=(8, 6))
    sns.barplot(x=user_distribution_by_gender.index, y=user_distribution_by_gender.values,
                hue=user_distribution_by_gender.index, palette='coolwarm', legend=False)
    plt.title('Distribución de Usuarios por Género')
    plt.xlabel('Género')
    plt.ylabel('Cantidad')
    plt.show()

    # Gráfico de distribución de ocupación
    plt.figure(figsize=(12, 6))
    sns.barplot(x=user_distribution_by_occupation.index, y=user_distribution_by_occupation.values,
                hue=user_distribution_by_occupation.index, palette='magma', legend=False)
    plt.title('Distribución de Usuarios por Ocupación')
    plt.xlabel('Ocupación')
    plt.ylabel('Cantidad')
    plt.xticks(rotation=90)
    plt.show()


# Función para insertar datos en SQL Server
def insert_data_sql(users_df, items_df, ratings_df):
    try:
        # Insertar datos en tabla Users
        users_df.to_sql('Users', engine, if_exists='replace', index=False)
        print("Datos de usuarios insertados con éxito en la tabla 'Users'.")

        # Insertar datos en tabla Movies
        items_df.to_sql('Movies', engine, if_exists='replace', index=False)
        print("Datos de películas insertados con éxito en la tabla 'Movies'.")

        # Insertar datos en tabla Ratings
        ratings_df.to_sql('Ratings', engine, if_exists='replace', index=False)
        print("Datos de valoraciones insertados con éxito en la tabla 'Ratings'.")

    except SQLAlchemyError as e:
        print(f"Error al insertar datos en la base de datos: {e}")


# Ejecutar el procesamiento de archivos
process_data_files()
