#Import libraries 
import findspark
import unicodedata
findspark.init()
import shapely
import pandas as pd
import geopandas as gpd
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

#Begin spark sesion
def begin_spark():
    spark = SparkSession. \
    builder. \
    appName('appName'). \
    config("spark.serializer", KryoSerializer.getName). \
    config("spark.executor.memory", "5g"). \
    config("spark.driver.memory", "10g"). \
    config('spark.driver.maxResultSize', '5g'). \
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
    config('spark.jars.packages',
            'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.2.0-incubating,org.datasyslab:geotools-wrapper:1.1.0-25.2'). \
    getOrCreate()
    SedonaRegistrator.registerAll(spark)
    return spark


def connection_municipal(spark):
    BD_MUNICIPAL = spark.read.parquet(f"BackendFlask/INEGIBot/input/SCINCE_Parquets_Municipal/*.parquet")
    BD_MUNICIPAL.cache()
    BD_MUNICIPAL.createOrReplaceTempView("municipios")


def connection_estatal(spark):
    DB_ESTATAL = spark.read.parquet(f"BackendFlask/INEGIBot/input/SCINCE_Parquets_Estatal/*.parquet")
    DB_ESTATAL.cache()
    DB_ESTATAL.createOrReplaceTempView("estados")


def clean_str(x, y):
    x = x.replace("%20", " " )
    x = (unicodedata.normalize('NFKD', x).encode('ASCII', 'ignore').strip().lower().decode('UTF-8'))
    y = y.replace("%20", " " )
    y = (unicodedata.normalize('NFKD', y).encode('ASCII', 'ignore').strip().lower().decode('UTF-8'))
    return x, y


def get_CVEGEO_mun(estado, municipio):
    df_mun = pd.read_csv('BackendFlask\INEGIBot\input\df_diccionario_mun.csv')
    clave = df_mun.query("NOM_ENT2 == '%s' and NOMGEO2 == '%s'" % (estado, municipio))
    clave = str(int(clave['CVEGEO']))
    return clave


def get_CVEGEO_est(estado):
    df_mun = pd.read_csv('BackendFlask\INEGIBot\input\df_diccionario_est.csv')
    clave = df_mun.query("NOMGEO2 == '%s'" % estado)
    clave = str(int(clave['CVEGEO']))
    return clave


def define_geo(json_flag, municipio_flag, spark):
    if municipio_flag == True:
        connection_municipal(spark)
        clave = get_CVEGEO_mun(json_flag['estado'], json_flag['municipio'])
        clave = clave.zfill(5)
        tabla = 'municipios'
        nombregeo = 'NOMGEO, NOM_ENT,'
    else:
        connection_estatal(spark)
        clave = get_CVEGEO_est(json_flag['estado'])
        clave = clave.zfill(2)
        tabla = 'estados'
        nombregeo = 'NOMGEO,'
    return clave, tabla, nombregeo


def pob_edad(clave, tabla, campos_pob, spark): #función para obtener array desglose por edad
    pob_m = spark.sql("""
    SELECT %s
    FROM %s 
    WHERE CVEGEO = '%s';
    """ % (campos_pob, tabla, clave))
    pob_m.cache()
    list_pob = pob_m.toPandas().to_numpy()
    list_pob = list_pob[0].tolist()
    return list_pob

def get_pob_message(clave, tabla, nombregeo, spark, campos_pob): #ajustar cambios para obtener pob_message y nomgeo
    pob_m = spark.sql("""
    SELECT %s %s
    FROM %s 
    WHERE CVEGEO = '%s';
    """ % (nombregeo,campos_pob, tabla, clave))
    pob_m.cache()
    pob_m = pob_m.toPandas().to_numpy()
    if tabla == 'estados':
        nom_message = str(pob_m[0][0])
        pob_message = add_commas(str(int(pob_m[0][1])))
        return nom_message, pob_message
    else:
        nom_message = str(pob_m[0][0]) + ', ' + str(pob_m[0][1])
        pob_message = add_commas(str(int(pob_m[0][2])))
        return nom_message, pob_message


def get_compatibles(filtro, desglose):
    if (filtro == 'Masculina' or filtro == 'Femenina') and desglose == 'Edad':
        compatible = True
    elif (filtro == 'Infantil' or filtro == 'Juvenil' or filtro == 'Adulta' or filtro == 'Tercera edad') and desglose == 'Sexo':
        compatible = True
    else:
        compatible = False
    return compatible

spark = begin_spark()
campos_pob_mujeres = 'POB44, POB68, POB69, POB70, POB71, POB72, POB73, POB74, POB75, POB76, POB77, POB78, POB58, POB79, POB80, POB81, POB82, POB83'
campos_pob_hombres = 'POB86, POB109, POB110, POB111, POB112, POB113, POB114, POB115, POB116, POB117, POB118, POB119, POB99, POB120, POB121, POB122, POB123, POB124'
campos_pob_general = 'POB3, POB26, POB27, POB28, POB29, POB30, POB31, POB32, POB33, POB34, POB35, POB36, POB16, POB37, POB38, POB39, POB40, POB41'
campos_sexo = 'POB42, POB84'
label_edad = ['0:4','5:9','10:14','15:19','20:24','25:29','30:34','35:39','40:44','45:49','50:54','55:59','60:64','65:69','70:74','75:79','80:84','85+']
label_sexo = ['Hombres', 'Mujeres']
campos_filtro_dic = {'Masculina':'POB84', 'Femenina':'POB42', 'Infantil':'POB8', 'Juvenil':'POB11', 'Adulta': '(POB14 + POB15) AS adulto', 'Tercera edad':'POB23'}
str_filtro_dic = {'Masculina':'masculina', 'Femenina':'femenina', 'Infantil':'infantil(0-14)', 'Juvenil':'juvenil(15-29)', 'Adulta': 'adulta(30-59)', 'Tercera edad':'tercera edad(60+)'}
#json_flag = {'municipio': 'Monterrey',  #Diccionario de pruebas para ingresar parametros
#    'estado': 'Nuevo Leon',
#    'filtro': 'adulto',  # lista disponible(hombre, mujer, infantil, joven, adulto, mayor)
#    'desglose': 'sexo'}  # lista diponible(edad, sexo)

def add_commas(n):
    n = str(n)
    if len(n) <= 3:
        return n
    else:
        return add_commas(n[:-3]) + ',' + n[-3:]

def flag_alert(json_flag, spark):
    json_flag['estado'], json_flag['municipio'] = clean_str(json_flag['estado'], json_flag['municipio'])
    df_mun = pd.read_csv('BackendFlask\INEGIBot\input\df_diccionario_mun.csv')
    filtro_list = ['Masculina', 'Femenina', 'Infantil', 'Juvenil', 'Adulta', 'Tercera edad']
    municipio_flag = (df_mun.NOMGEO2 == json_flag['municipio']).any()
    filtro_flag = json_flag['filtro'] in filtro_list
    desglose_flag = json_flag['desglose'] in ['Edad', 'Sexo']
    clave, tabla, nombregeo = define_geo(json_flag, municipio_flag, spark)  # Se obtienen la clave del municipio/estado y su nombre
    compatible = get_compatibles(json_flag['filtro'], json_flag['desglose'])  # variable para determinar si es compatible el filtro con el desglose en caso negativo se muestra solo el filtro
    if filtro_flag == False and desglose_flag == False: #doble desglose general
        data_pob_muj = pob_edad(clave, tabla, campos_pob_mujeres, spark)
        data_pob_hom = pob_edad(clave, tabla, campos_pob_hombres, spark)
        nom_message, pob_message = get_pob_message(clave, tabla, nombregeo, spark, 'POB1')
        messages = to_json_doble_desglose(nom_message, pob_message, label_edad, data_pob_hom, data_pob_muj)
        return messages
    elif filtro_flag == False and desglose_flag == True: #desglose simple
        if json_flag['desglose'] == 'Edad':  # aplicando desglose por edad
            data_pob_gen = pob_edad(clave, tabla, campos_pob_general, spark)
            nom_message, pob_message = get_pob_message(clave, tabla, nombregeo, spark, 'POB1')
            message_str = "En 2020 la poblacion de %s era de %s personas. A continuacion se muestra la poblacion desglosada por edad." % (nom_message, pob_message)
            messages = to_json_simple_desglose(message_str, label_edad, data_pob_gen)
            return messages
        else:  # aplicando desglose por sexo
            data_pob_sex = pob_edad(clave, tabla, campos_sexo, spark) 
            nom_message, pob_hombres_message = get_pob_message(clave, tabla, nombregeo, spark, 'POB84')
            nom_message, pob_mujeres_message = get_pob_message(clave, tabla, nombregeo, spark, 'POB42')
            message_str = "En 2020 la poblacion masculina en %s era de %s y la femenina era de %s." % (nom_message,pob_hombres_message, pob_mujeres_message)
            messages = to_json_simple_desglose(message_str, label_sexo, data_pob_sex)
            return messages
    elif filtro_flag == True and desglose_flag == True and compatible == True: # aplicando filtro y desglose
        campos_filtro = campos_filtro_dic[json_flag['filtro']] # campos necesarios para cada filtro
        if json_flag['desglose'] == 'Edad':  # aplicando desglose por edad
            if json_flag['filtro'] == 'Masculina':
                data_pob = pob_edad(clave, tabla, campos_pob_hombres, spark)
            if json_flag['filtro'] == 'Femenina':
                data_pob = pob_edad(clave, tabla, campos_pob_mujeres, spark)
            nom_message, pob_filtro_message = get_pob_message(clave, tabla, nombregeo, spark, campos_filtro)
            message_str = "En 2020 la poblacion %s en %s era de %s. A continuación se muestra desglosada por edad." % (str_filtro_dic[json_flag['filtro']], nom_message, pob_filtro_message)
            messages = to_json_simple_desglose(message_str, label_edad, data_pob)
            return messages
        if json_flag['desglose'] == 'Sexo':
            if json_flag['filtro'] == 'Infantil':
                campos_rango_edad, campos_filtro_sexo = ['POB91', 'POB49'], 'POB91, POB49'
            elif json_flag['filtro'] == 'Juvenil':
                campos_rango_edad, campos_filtro_sexo = ['POB94', 'POB52'], 'POB94, POB52'
            elif json_flag['filtro'] == 'Adulta':
                campos_rango_edad, campos_filtro_sexo = ['(POB97 + POB98) AS adulto', '(POB56 + POB57) AS adulto'], '(POB97 + POB98) AS adulto, (POB56 + POB57) AS adulto'
            elif json_flag['filtro'] == 'Tercera edad':
                campos_rango_edad, campos_filtro_sexo = ['POB106', 'POB65'], 'POB106, POB65'
            data_pob_sex = pob_edad(clave, tabla, campos_filtro_sexo, spark) 
            nom_message, pob_hombres_message = get_pob_message(clave, tabla, nombregeo, spark, campos_rango_edad[0])
            nom_message, pob_mujeres_message = get_pob_message(clave, tabla, nombregeo, spark, campos_rango_edad[1])
            message_str = "En 2020 la poblacion %s masculina en %s era de %s y la femenina era de %s." % (str_filtro_dic[json_flag['filtro']], nom_message, pob_hombres_message, pob_mujeres_message)
            messages = to_json_simple_desglose(message_str, label_sexo, data_pob_sex)
            return messages
    elif filtro_flag == True: # aplicando filtro sin desglose 
        campos_filtro = campos_filtro_dic[json_flag['filtro']] # campos necesarios para cada filtro
        nom_message, pob_filtro_message = get_pob_message(clave, tabla, nombregeo, spark, campos_filtro)
        message_str = "En 2020 la poblacion %s en %s era de %s." % (str_filtro_dic[json_flag['filtro']], nom_message,pob_filtro_message)
        messages = to_json_simple_message(message_str)
        return messages
    


def to_json_doble_desglose(nom_message, pob_message, label_edad, data_pob_hom, data_pob_muj):
    message_json = {"type": "graph_2",
            "message":"En 2020 la poblacion de %s era de %s personas. A continuacion se muestra la poblacion desglosada por edad y sexo." % (nom_message, pob_message),
            "graph": {
                "labels": label_edad,
                "datasets": [{
                        "label": "Hombres",
                        "data":data_pob_hom,
                        "backgroundColor": "rgb(1, 67, 143)",
                        "borderColor": "rgb(1, 50, 107)"
                    },
                    {
                        "label": "Mujeres",
                        "data":data_pob_muj,
                        "backgroundColor": "rgb(255, 99, 132)",
                        "borderColor": "rgb(191, 74, 99)"
                    }
                ]
            }
    }
    return message_json


def to_json_simple_desglose(message_str, label, data_pob):
    message_json = {
        "type": "graph_1",
        "message":message_str,
        "graph":{
            "labels": label,
            "datasets": [{
                    "label": "Poblacion",
                    "data":data_pob,
                    "backgroundColor": "rgb(0, 119, 200)",
                    "borderColor": "rgb(0, 89, 150)"
                    }
                ]
            }   
        }
    return message_json


def to_json_simple_message(message_str):
    message_json = {
        "type": "simple",
        "message":message_str
        }
    return message_json



#messages = flag_alert(json_flag, spark)
#print(messages)
