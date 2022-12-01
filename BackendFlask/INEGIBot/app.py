from random import random
from flask import Flask, send_file
from flask_cors import CORS
from spark_nacional import *
from flags import *
import json
import requests


app = Flask(__name__)
CORS(app)

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


@app.route('/query/<input>', methods = ['GET'])
def query(input): 
    neuraan_json = {'results': {}}
    payload = json.dumps({
        "name": "Detección de entidades",
        "input": {
            "text": input,
        }
    })
    headers = {
    'Authorization': 'Bearer eyJpZCI6IjQ3MzBjYmRhLWViNzYtNDVjYS04MTI0LWM4YzlkZjRmMDEyOCIsImlhdCI6MTY2Nzk0OTA3NH0',
    'Content-Type': 'application/json'
    }
    try:
        response = requests.request("POST", "https://invoke.neuraan.com/default/v1", headers=headers, data=payload)
        neuraan_json = response.json()
    except requests.ConnectionError:
        pass
    
    # if response.status_code == 200:
    #     neuraan_json = response.json()
        
    
    master_json = {
    'municipio': 'none',  #Diccionario de pruebas para ingresar parametros
    'estado': 'none',
    'filtro': 'none',  # lista disponible(hombre, mujer, infantil, joven, adulto, mayor)
    'desglose': 'none'
    }
    
    messages = {
        "type": "simple",
        "message": "Hubo un error con la consulta, favor de verificar el formato del mensaje con los lineamientos en el boton de ayuda."
        }
    
    # with open('.\BackendFlask\INEGIBot\master_json_example_2.json', encoding='utf-8') as json_file:
    #     neuraan_json = json.loads(json_file.read(), )
     
    entities = []   
    cities = []
    states = []
    filter_sex = []
    filter_age = []
    breakdown = []

    double_city_flag = False
    single_city_flag = False
    invalid_location_flag = False
    no_filter_flag = False
    no_breakdown_flag = False
    city_count = 0
    state_count = 0


    if neuraan_json['result'] != {}:
        if neuraan_json['result']['detected'] != []:
            df_estados = pd.read_csv('.\BackendFlask\INEGIBot\input\df_diccionario_est.csv')
            for i in neuraan_json['result']['detected']:
                entities.append({
                    'entity': i[0],
                    'value': i[2]
                })
                
            cities = [x['value'] for x in entities if x['entity'] == 'CIUDAD']
            states = [x['value'] for x in entities if x['entity'] == 'ESTADO']
            filter_sex = [x['value'] for x in entities if x['entity'] == 'FILTRO_SEXO']
            filter_age = [x['value'] for x in entities if x['entity'] == 'FILTRO_GRUPO_EDAD']
            breakdown = [x['value'] for x in entities if x['entity'] == 'DESGLOSE']
                            
            if len(cities) > 1:
                double_city_flag = True
            if len(cities) == 1 and len(states)  == 0:
                single_city_flag = True
            if (len(cities) == 0 and len(states)  == 0) or (len(states)  > 1) or (len(cities) > 2) or (len(filter_age) + len(filter_sex) > 1) or (len(breakdown) > 1):
                invalid_location_flag = True
            if (len(filter_sex) == 0 and len(filter_age) == 0):
                no_filter_flag = True
            if len(breakdown) == 0:
                no_breakdown_flag = True                 
        else:
            invalid_location_flag = True
    else:
        invalid_location_flag = True

    if not invalid_location_flag:
        if double_city_flag:
            if len(cities) == 2:
                if cities[0] == cities[1]:
                    if cities[0] == 'Veracruz':
                        master_json['municipio'] = 'Veracruz'
                        master_json['estado'] = 'Veracruz de Ignacio de la Llave'
                    elif cities[0] in df_estados['NOMGEO'].values:
                        master_json['municipio'] = cities[0]
                        master_json['estado'] = cities[0]
                    else:
                        invalid_location_flag = True
                else :
                    invalid_location_flag = True      
        elif single_city_flag:
            city = cities[0]
            if city in df_estados['NOMGEO'].values:
                master_json['estado'] = city
            elif city == 'Veracruz':
                master_json['municipio'] = 'Veracruz'
                master_json['estado'] = 'Veracruz de Ignacio de la Llave'
            else:
                invalid_location_flag = True
        else:
            if states[0] == 'Coahuila':
                master_json['estado'] = 'Coahuila de Zaragoza'
            elif states[0] == 'Michoacán':
                master_json['estado'] = 'Michoacán de Ocampo'
            else:
                master_json['estado'] = states[0]
            
            if len(cities) == 1:
                master_json['municipio'] = cities[0]
                
            

        #"FILTRO_SEXO":["Masculina","Femenina"],
        #"FILTRO_GRUPO_EDAD":["Infantil","Juvenil","Adulta","Tercera Edad"],
        # "DESGLOSE":["Sexo","Edad"]
        if not invalid_location_flag:
            filter_dict = {'Masculina':'hombre', 'Femenina':'mujer', 'Infantil':'infantil', 'Juvenil':'joven', 'Adulta':'adulto', 'Tercera Edad':'mayor'}
            breakdown_dict = {'Sexo':'sexo', 'Edad':'edad'}
            if not no_filter_flag:
                if len(filter_sex) == 1:
                    master_json['filtro'] = filter_sex[0]
                else:
                    master_json['filtro'] = filter_age[0]
                    
            if not no_breakdown_flag:
                master_json['desglose'] = breakdown[0]
                            
            df_mun = pd.read_csv('.\BackendFlask\INEGIBot\input\df_diccionario_mun.csv')
            if (not df_mun.loc[(df_mun['NOM_ENT'] == master_json['estado']) & (df_mun['NOMGEO'] == master_json['municipio'])].empty) or (master_json['municipio'] == 'none' and master_json['estado'] != 'none'):
                messages = flag_alert(master_json, spark)
            
    #request a neuraan
    
    
    #nom_e, nom_m, pob_m = pob_municipio(estado, municipio, spark)
    #nom_e, nom_m, pob_m = pob_municipio('Nuevo Leon', 'Monterrey', spark)
    # print(messages)
    return messages
    
if __name__ == '__main__':
    spark = begin_spark()
    app.run(debug=False)