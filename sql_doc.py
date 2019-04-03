"""
来源
http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html
"""
from pyspark.sql import SQLContext,DataFrameStatFunctions,SparkSession,WindowSpec,DataFrameNaFunctions,UDFRegistration
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.dataframe import *
import os

"""
sparksession 
处理rdd 和df的入口

"""


# sparksession.createDataFrame

'''
 SparkSession.createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)
'''
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.7'
def create_spark(name='firstdemo'):
    """
    生产sparksession实例
    :return:
    """
    url='local'
    spark = SparkSession \
        .builder \
        .master(url) \
        .appName(name) \
        .getOrCreate()
    ctx = SQLContext(spark)
    return spark, ctx

l=[('Alice',1),('cyy',18)]
spark,ctx=create_spark()
df=spark.createDataFrame(l)
print(df.schema )
df.show()
