#Import libraries 
import findspark
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

    #Establish connection to db
    BD_NACIONAL = spark.read.parquet(f"BackendFlask/INEGIBot/input/SCINCE_Parquets_Nacional/*.parquet")
    #BD_NACIONAL = BD_NACIONAL.select('CVEGEO', 'ECO1_R', 'EDU46_R', 'VIV82_R', 'VIV83_R', 'VIV84_R', 'geometry') *In case of 
    BD_NACIONAL.cache()
    BD_NACIONAL.printSchema()
    BD_NACIONAL.show()


begin_spark()
BD_NACIONAL.createOrReplaceTempView("nacional_a")
BD_NAL_TESTER = spark.sql("""
SELECT CVEGEO, ECO1_R, EDU46_R, VIV82_R, VIV83_R, VIV84_R, geometry 
FROM nacional_a;
""")
BD_NAL_TESTER.cache()
BD_NAL_TESTER.show()
