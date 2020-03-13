# Script completo de la actividad AG1 del módulo 06MIOT.

import urllib.request
import json
import pprint
import pymongo
import random

def create_json(out_file, data): # se abre un archivo y se vuelca en el mismo los datos del diccionario
    file_data = open(out_file, 'w')
    with open(out_file, 'w') as file:
        json.dump(data, file, indent=4)
    file_data.close()

def download_data(url_data):    # se exportan los datos a un objeto diccionario
    with urllib.request.urlopen(url_data) as url:
        data = json.loads(url.read().decode())
    return data

def mongo_connect():
    print("Connecting to MongoDB...")
    uri_mongo = "mongodb+srv://adria:adria@cluster0-1yjkn.mongodb.net/test?retryWrites=true&w=majority"
    client = pymongo.MongoClient(uri_mongo)
    client.test
    return client

def read_json(json_path):
    with open(json_path) as f:
        file_data = json.load(f)
    return file_data

def insert_mongo_list(list_data, mongo_client, database, collection):
    # first define the collection to insert
    db = mongo_client[database]
    col = db[collection]
    # insert array of Features into collection
    col.insert_many(list_data["features"])
    #pprint.pprint(list_data["features"])
    longitud_array = len(list_data["features"])
    print('\n' + repr(longitud_array) + ' documentos insertados en la Colección ' + collection)

def add_random_numeric(your_collection):
    print("Updating collection in order to add a random numeric...")
    """cursor = your_collection.find()
    for rs in cursor:
        your_collection.update_one(
            {condicion},
            [
                {"$set": {"numerico_inventado": random.randint(1, 100)}}
            ]
        )"""
    your_collection.update_many(
        {},
        [
            {"$set": {"numerico_inventado": random.randint(1, 100)}}
        ]
    )

"""Mediante una consulta a Mongo obtener unos campos concretos de una consulta buscando en un atributo un valor 
concreto. El valor tiene que ser un valor numérico."""
def get_query1(collection):
    # devuelve la Descripción del Recurso Social cuya coordenada Y sea la dada
    return collection.find_one({"geometry.coordinates.1":4371138.878},{"properties.descripcion":1})

"""Hacer una consulta de Mongo de forma que nos devuelva, en función de un atributo, el top 3 ordenado."""
def get_query2(collection):
    # devuelve los 3 primeros Recursos Sociales sin Titularidad, ordenados ascendentemente por Descripción
    return collection.find({"properties.titularidad": ""}).sort("properties.descripcion",1).limit(3)

"""Existe el atributo “coordinates” en el conjunto de datos? ¿Cómo se consulta?"""
def get_query2b(collection, coordenada):
    # devuelve el documento cuya coordenada x coincida con la dada. Muestra solo las Coordenadas.
    return collection.find_one({"geometry.coordinates.0":coordenada}, {"_id":0,"geometry.coordinates":1})

"""Recorrer todos los registros de la colección y realizar una media en un atributo."""
def get_query3(collection):
    # devuelve la media del atributo "numerico_inventado" de todos los documentos de la colección
    return collection.aggregate(
        [
            {"$group": {"_id": {}, "Promedio numerico_inventado": {"$avg": "$numerico_inventado"}}}
        ])

# Programa principal:
if __name__ == "__main__":

    # PRIMER PASO: crear fichero JSON a partir de la fuente online de datos abiertos

    url_data = ' http://mapas.valencia.es/lanzadera/opendata/ssdiscapacidad/JSON'
    name_file_out = './rrss_discapacidad.json'
    rrss = download_data(url_data)
    create_json(out_file=name_file_out, data=rrss)
    #pprint.pprint(rrss)
    print("\n*********Fichero JSON creado\n")

    # SEGUNDO PASO: Conectar a MongoDB e insertar datos

    # Parameters
    db_name = "db_AG1"
    collection_name = "rrss_discapacidad"
    json_file = './rrss_discapacidad.json'

    # mongoDB connection
    cli = mongo_connect()

    # database and collection
    db = cli[db_name]
    cole = db[collection_name]

    # read data from file
    ls_json = read_json(json_path=json_file)

    # and insert it into collection
    insert_mongo_list(list_data=ls_json, mongo_client=cli, database=db_name, collection=collection_name)

    # add new numeric field to collection because there was no one.
    add_random_numeric(cole)

    # TERCER PASO: Realizar consultas sobre MongoDB

    # get data
    print("\nQUERY 1: ")
    q = get_query1(cole)
    pprint.pprint(q)

    print("QUERY 2: ")
    q = get_query2(cole)
    for rs in q:
        pprint.pprint(rs)

    print("\nQUERY 2b: ")
    q = get_query2b(cole, 724111.861)
    pprint.pprint(q)

    print("\nQUERY 3: ")
    q = get_query3(cole)
    for rs in q:
        pprint.pprint(rs)

    # FINALIZAMOS

    # close mongoDB connection
    cli.close()

    print("\nFinishing script...")