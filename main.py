# Se importan las bibliotecas a utilizar
from fastapi import FastAPI
import pandas as pd

# Se instancia la clase de API
app = FastAPI()

# Ruta de los archivos
ruta_games = "./data/r_games.json.gzip"
ruta_reviews = "./data/r_reviews.json.gzip"
ruta_items = "./data/r_items.json.gzip"

# Se cargan las bibliotecas de datos
games = pd.read_json(ruta_games,compression="gzip",convert_dates=["release_date"],date_unit="ms")
reviews = pd.read_json(ruta_reviews,compression="gzip",convert_dates=["posted"],date_unit="ms")
items = pd.read_json(ruta_items,compression="gzip")

# Se crean las consultas

@app.get("/developer/{desarrollador}")
def developer(desarrollador:str):
    # Se traen los datos que coinciden con el desarrollador
    h =  games.loc[games["developer"] == desarrollador]
    # Convertir la columna "release_date" a tipo datetime
    h.loc[:,"release_date"] = pd.to_datetime(h["release_date"])

    # Se utliza el atributo dt para extraer el año y luego agrupar por año y número de juegos.
    x = h.groupby(games["release_date"].dt.year)["app_name"].count()

    # Se convierte a dataframe la agrupación resultante.
    r = x.reset_index()

    # Se cambian los nombres de las columnas.
    r.rename(columns={"release_date":"Año","app_name":"Cantidad de Items"},inplace=True)

    # Se agrupa por desarrollador y año, contar la cantidad de juegos gratuitos ("Free") por año
    f = h.groupby(h["release_date"].dt.year)["price"].apply(lambda x: (x == "Free").sum()).reset_index()

    # Se combinan los resultados de las agrupaciones anteriores.
    r = r.merge(f,left_on="Año",right_on="release_date")

    # Se obtiene el % del contenido free por año.
    r["Contenido Free (%)"] = round(r["price"]/r["Cantidad de Items"],2)*100

    # Se eliminan las columnas que no se van a utilizar.
    r.drop(columns=["release_date","price"],inplace=True)

    respuesta = []

    # Se itera sobre las filas del DataFrame r para obtener las respuestas
    for index, row in r.iterrows():
        # Se construye un diccionario con los datos de cada fila
        item = {
            "año": row["Año"],
            "cantidad de items": row["Cantidad de Items"],
            "Contenido Free (%)": row["Contenido Free (%)"]
        }
        # Agregar el diccionario a la lista de respuestas
        respuesta.append(item)

    # Se da el resultado.
    return respuesta

@app.get("/userdata/{user}")
def userdata(user:str):

    # Se obtienen los items para el jugador especificado.
    b = items[items["user_id"] == user]

    # Se obtiene el número de juegos para el usuario. 
    n_juegos = int(b.loc[b.index[0],"items_count"])

    # Se obtienen los juegos del usuario
    lista_juegos = b["item_id"].tolist()

    # Se seleccionan los juegos que pertenecen al usuario y que no son "Free"
    g = games[(games["id"].isin(lista_juegos)) & (games["price"] != "Free")]
    # Se convierte la columna "price" a tipo numérico
    g.loc[:,"price"] = pd.to_numeric(g["price"], errors="coerce")
    # Se suman los precios
    money = g["price"].sum()

    # Se obtienen el % de juegos recomendados.
    n_recom = len(reviews[(reviews["user_id"] == user) & (reviews["recommend"] == True)])
    recomendacion = float(round(n_recom/n_juegos,3)*100)

    #Se obtiene la respuesta
    respuesta = {"Usuario":user,
                "Dinero gastado (USD)":money,
                "% de recomendación":recomendacion,
                "Cantidad de items":n_juegos}
    
    return respuesta

@app.get("/UserForGenre/{genero}")
def UserForGenre(genero:str):
    # Se eliminan las columnas innecesarias del dataframe de games.
    g = games.drop(columns=["app_name","developer","price"])
    # Se "explotan" los géneros por juego.
    g = g.explode("genres")
    # Se filtran los juegos para que sólo queden aquellos que cumplen el género indicado.
    g = g[g["genres"] == genero][["release_date", "id"]]
    # Se convierte la fecha a año.
    g["release_date"] = g["release_date"].dt.year

    # Se obtienen los id de los juegos que cumplen con el género.
    games_id = g["id"].unique()

    # Se obtienen los juegos del dataframe items que corresponden al género indicado.
    i = items.drop(columns=["items_count"])
    i = i[i["item_id"].isin(games_id)]

    # Se combinan los dataframes para obtener los años de los juegos.
    s = pd.merge(left = i,right=g,left_on="item_id",right_on="id")
    # Se obtiene la sumatoria de horas por año y por usuario.
    s = s.groupby(["user_id","release_date"])["playtime_forever"].sum()

    # Se obtiene el jugador con más horas jugadas.
    player = s.idxmax()[0]

    # Se obtiene la información para el jugador identificado.
    player_data = s.loc[player]

    # Se estructura la respuesta: 
    response_data = {
        "genero":genero,
        "jugador_con_mas_horas":player,
        "datos": [{"año":year,
                        "horas":playtime
                        }
                        for year,playtime in player_data.items()
                        ]
    }

    return response_data

@app.get("/developer_reviews_analysis/{desarrolladora}")
def developer_reviews_analysis(desarrolladora:str):
    # Se obtienen todos los juegos de la desarrolladora.
    g = games[games["developer"] == desarrolladora]["id"].tolist()

    # Se obtienen todos los juegos en reviews que son de la desarrolladora
    r = reviews[reviews["item_id"].isin(g)][["item_id","sentiment_analysis"]]

    # Se cuenta el total de reseñas positivas
    positivas = len(r[r["sentiment_analysis"] == 2])

    # Se cuenta el total de reseñas negativas
    negativas = len(r[r["sentiment_analysis"] == 0])

    # Se obtiene el resultado
    resultado = {desarrolladora:[
        f"Negative = {negativas}",
        f"Positive = {positivas}"]}

    return resultado

@app.get("/best_developer_year/{año}")
def best_developer_year(año:int):
    # Se obtienen los juegos y los desarrolladores de aquellos juegos que fueron lanzados en el año identificado.
    g = games[games["release_date"].dt.year == año][["release_date","id","developer"]]
    # Se crea ima columna con el año de la fecha de lanzamiento.
    g["year"] = g["release_date"].dt.year
    # Se obtiene el id de los juegos
    juegos = g["id"].to_list()

    # se obtienen los juegos recomendados que salieron en el año indicado
    r = reviews[(reviews["item_id"].isin(juegos) & (reviews["recommend"] == True))][["item_id","recommend"]]
    # Se obtiene el número de recomendaciones por juego.
    recomendaciones = r.groupby("item_id").count()

    # Se une el número de recomendaciones por juegos al dataframe con los desarrolladores para cada juego.
    s = recomendaciones.merge(g[["id","developer"]],left_on="item_id",right_on="id",how='left')
    # Se agrupan las recomendaciones por desarrolladora
    s = s.groupby("developer")["recommend"].sum()
    # Se organizan de mayor a menor
    s = s.sort_values(ascending=False)

    # Se obtienen los 3 mejores desarrolladores
    mejores = s.head(3)

    # Se sacan los resultados
    resultado = []
    puesto = 1

    # Se obtiene el puesto por desarrolladora y se agregan a la variable de resultado
    for desarrolladora, recom in mejores.items():
        resultado.append({"puesto":puesto,
                        "developer":desarrolladora,
                        })
        puesto += 1
    
    return resultado