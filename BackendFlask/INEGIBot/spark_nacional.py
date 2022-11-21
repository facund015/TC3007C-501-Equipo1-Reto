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
    #BD_NACIONAL = BD_NACIONAL.select('CVEGEO', 'ECO1_R', 'EDU46_R', 'VIV82_R', 'VIV83_R', 'VIV84_R', 'geometry') *In case of 
    BD_MUNICIPAL.cache()
    BD_MUNICIPAL.createOrReplaceTempView("estados")


def clean_str(x):
    x = (unicodedata.normalize('NFKD', x).encode('ASCII', 'ignore').strip().lower().decode('UTF-8'))
    return x


def get_CVEGEO_mun(municipio):
    #df_mun = pd.read_csv('input/df_diccionario_mun.csv')
    df_mun = pd.read_csv('BackendFlask\INEGIBot\input\df_diccionario_mun.csv')
    clave = df_mun.loc[:, 'NOMGEO2'] == municipio
    clave = str(int(df_mun.loc[clave]['CVEGEO']))
    return clave


#spark = begin_spark()
#Establish connection to db

#BD_NACIONAL.printSchema()
#BD_NACIONAL.show()
def pob_municipio(municipio, spark):
    connection_municipal(spark)
    municipio = clean_str(municipio)
    municipio = get_CVEGEO_mun(municipio)
    pob_m = spark.sql("""
    SELECT POB1, NOMGEO
    FROM estados
    WHERE CVEGEO = '%s';
    """ % municipio)
    pob_m.cache()
    pob_m = pob_m.toPandas().to_numpy()
    nom_m = str(pob_m[0][1])
    pob_m = str(pob_m[0][0])
    return pob_m, nom_m

#spark = begin_spark()
#aa, bb = pob_municipio('Monterrey', spark)
#print(aa, bb)
#print(pob_municipio('Monterrey', spark))