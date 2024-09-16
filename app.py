import pandas as pd
# import pyodbc
'''
# Conexión a SQL Server
conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=localhost;'
    'DATABASE=lab06db;'
    'UID=sa;'
    'PWD=Huevos1'
)
cursor = conn.cursor()
'''

# Leer archivos de datos
def process_data_files():

    # 1. Leer 'u.genre' (GENERO)
    genre_df = pd.read_csv('./ml-100k/u.genre', sep='|', names=['genre_name','id'])
    print(genre_df.head())

    # 2. Leer 'u.occupation' (OCUPACION)
    occupation_df = pd.read_csv('./ml-100k/u.occupation', sep='|', names=['occupation_name'])
    print(occupation_df.head())

    # 3. Leer 'u.user' (información demográfica)
    users_df = pd.read_csv('./ml-100k/u.user', sep='|', names=['user_id', 'age', 'gender', 'occupation', 'zip_code'])
    print(users_df.head())  # Verificar los primeros registros

    # 4. Leer 'u.item' (información sobre películas)
    items_columns = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url'] + [f'genre_{i}' for i in range(19)]
    items_df = pd.read_csv('ml-100k/u.item', sep='|', names=items_columns, encoding='ISO-8859-1')
    print(items_df.head())  # Verificar los primeros registros

    # Unir las columnas de géneros en una sola columna de "géneros"
    genres_columns = items_columns[5:]
    items_df['genres'] = items_df[genres_columns].apply(lambda row: ','.join([str(i) for i, val in enumerate(row) if val == 1]), axis=1)
    items_df = items_df[['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url', 'genres']]

    # 5. Procesar 'u.data' (valoraciones)
    ratings_df = pd.read_csv('ml-100k/u.data', sep='\t', names=['user_id', 'movie_id', 'rating', 'timestamp'])
    ratings_df['timestamp'] = pd.to_datetime(ratings_df['timestamp'], unit='s')  # Convertir la marca de tiempo a formato datetime
    print(ratings_df.head())  # Verificar los primeros registros

    # 6. Leer 'u.info' (INFORMACION)
    info_df = pd.read_csv('ml-100k/u.info', sep=' ', names=['cant.', 'type'])
    print(info_df.head())

    return users_df, items_df, ratings_df


process_data_files()