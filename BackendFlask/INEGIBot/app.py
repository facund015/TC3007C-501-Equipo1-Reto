from random import random
from flask import Flask, send_file
from flask_cors import CORS
from spark_nacional import *
from flags import *

app = Flask(__name__)
CORS(app)

@app.route('/message/<message>', methods = ['GET'])
def hello_world(message):
    random_number = int(random()*4)
    
    print(message)
    
    messages = [
        {
            "type": "simple",
            "message":"En 2020 la poblacion de Monterrey era de 5,784,442 personas"
            },
        {
            "type": "simple",
            "message":"En 2020 la poblacion de mujeres en Monterrey era de 2,893,492"
            },
        {
            "type": "graph_1",
            "message":"En 2020 la poblacion de hombres en Monterrey era de 2,890,950 y la de mujeres era de 2,893,492",
            "graph":{
                "labels": ["Hombres", "Mujeres"],
                "datasets": [{
                        "label": "Poblacion",
                        "data":[2890950, 2893492],
                        "backgroundColor": ["rgb(1, 67, 143)", "rgb(255, 99, 132)"]
                        }
                    ]
                }   
            },
        {
            "type": "graph_2",
            "message":"A continuacion se muestra la poblacion desglosada por edad y sexo",
            "graph": {
                "labels": ["0-14", "15-64", "65+"],
                "datasets": [{
                        "label": "Hombres",
                        "data":[10000, 10000, 10000],
                        "backgroundColor": "rgb(1, 67, 143)"
                    },
                    {
                        "label": "Mujeres",
                        "data":[20000, 20000, 20000],
                        "backgroundColor": "rgb(255, 99, 132)"
                    }
                ]
            }
        }
    ]
    
    return messages[random_number]
    

@app.route('/pob_municipal/<estado>/<municipio>', methods = ['GET'])
def hello_world2(estado, municipio):
    nom_e, nom_m, pob_m = pob_municipio(estado, municipio, spark)
    #nom_e, nom_m, pob_m = pob_municipio('Nuevo Leon', 'Monterrey', spark)
    print(estado, municipio)
    
    messages = [
        {
            "type": "simple",
            "message":"En 2020 la poblacion de %s ,%s era de %s personas" % (nom_m, nom_e, pob_m)
            }
    ]
    
    return messages[0]


@app.route('/master_funct/<master_json>', methods = ['GET'])
def pandora(master_json):
    
    if master_json == '1':
        master_json = {'municipio': 'none',  #Diccionario de pruebas para ingresar parametros
        'estado': 'Nuevo Leon',
        'filtro': 'none',  # lista disponible(hombre, mujer, infantil, joven, adulto, mayor)
        'desglose': 'none'}  # lista diponible(edad, sexo)
    elif master_json == '2':
        master_json = {'municipio': 'Monterrey',
        'estado': 'Nuevo Leon',
        'filtro': 'hombre',
        'desglose': 'none'}
    elif master_json == '3':
        master_json = {'municipio': 'Monterrey',
        'estado': 'Nuevo Leon',
        'filtro': 'none',
        'desglose': 'edad'}
    elif master_json == '4':
        master_json = {'municipio': 'Monterrey',
        'estado': 'Nuevo Leon',
        'filtro': 'none',
        'desglose': 'sexo'}
    elif master_json == '5':
        master_json = {'municipio': 'Monterrey',
        'estado': 'Nuevo Leon',
        'filtro': 'hombre',
        'desglose': 'edad'}
        
    
    #request a neuraan
    
    messages = flag_alert(master_json, spark)
    #nom_e, nom_m, pob_m = pob_municipio(estado, municipio, spark)
    #nom_e, nom_m, pob_m = pob_municipio('Nuevo Leon', 'Monterrey', spark)
    print(messages)
    return messages
    
if __name__ == '__main__':
    spark = begin_spark()
    app.run(debug=False)