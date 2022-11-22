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
    BD_MUNICIPAL.createOrReplaceTempView("estados")


def clean_str(x, y):
    x = x.replace("%20", " " )
    x = (unicodedata.normalize('NFKD', x).encode('ASCII', 'ignore').strip().lower().decode('UTF-8'))
    y = y.replace("%20", " " )
    y = (unicodedata.normalize('NFKD', y).encode('ASCII', 'ignore').strip().lower().decode('UTF-8'))
    return x, y


def get_CVEGEO_mun(estado, municipio):
    df_mun = pd.read_csv('BackendFlask\INEGIBot\input\df_diccionario_mun.csv')
    #clave = df_mun.loc['NOMGEO2' == municipio and 'NOM_ENT2' == estado ]
    #clave = str(int(df_mun.loc[clave]['CVEGEO']))
    clave = df_mun.query("NOM_ENT2 == '%s' and NOMGEO2 == '%s'" % (estado, municipio))
    clave = str(int(clave['CVEGEO']))
    return clave


def pob_municipio(estado, municipio, spark):
    connection_municipal(spark)
    estado, municipio = clean_str(estado, municipio)
    clave = get_CVEGEO_mun(estado, municipio)
    clave = clave.zfill(5)
    pob_m = spark.sql("""
    SELECT POB1, NOMGEO, NOM_ENT
    FROM estados
    WHERE CVEGEO = '%s';
    """ % clave)
    pob_m.cache()
    pob_m = pob_m.toPandas().to_numpy()
    nom_e = str(pob_m[0][2])
    nom_m = str(pob_m[0][1])
    pob_m = str(pob_m[0][0])
    return nom_e, nom_m, pob_m

spark = begin_spark()
nom_e, nom_m, pob_m = pob_municipio('Oaxaca', 'Abejones', spark)
print('La población del municipio de ' + str(nom_m) + ', ' + str(nom_e) + ' es: '+str(pob_m))
#print(pob_municipio('Monterrey', spark))