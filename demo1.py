from pyspark.sql import SparkSession, SQLContext, Window
import os
import pyspark.sql.functions as F

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType,DataType
from datetime import datetime
from pyspark.sql.functions import udf, Column


from conf.conf import con

# from conf import con

url = 'local'

"""
设置python环境变量
"""
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.7'


def create_spark(name='firstdemo'):
    """
    生产sparksession实例
    :return:
    """
    spark = SparkSession \
        .builder \
        .master(url) \
        .appName(name) \
        .getOrCreate()
    ctx = SQLContext(spark)
    return spark, ctx


def conf_spark(spark):
    """
    配置spark
    :return:
    """
    spark.conf.set("spark.executor.memmory", "500M")
    spark.conf.set("spark.cores.max", "2")
    spark.conf.set("spark.driver.maxResultSize", "2g")


def register_db(ctx, table, user, url, password, registertable):
    """
    连接mysql数据库,注册table
    :return:
    """
    # table='t_exer_record_0'
    driver = 'com.mysql.cj.jdbc.Driver'

    df = ctx.read.format("jdbc").options(url=url, driver=driver, dbtable=table,
                                         user=user, password=password).load()

    df.createOrReplaceTempView(registertable)


def create_df_from_sql(spark):
    """
    数据库读取数据产生df
    :param spark:
    :return:
    """
    sql = '''select user_id,duration,subject_1,item_id,create_time  from table_0 where channel=1 and user_id in (select user_id from usertable where if_delete=0) '''

    dbdf = spark.sql(sql)
    dbdf.show()

def write_csv(spark):
    """
    数据库读取数据并保存到csv
    :param spark:
    :return:
    """
    register_db(ctx=ctx, url=con['url'],table=con['table'], password=con['password'], user=con['user'], registertable='mytable')
    sql = '''select user_id,duration,subject_1,item_id,create_time from mytable '''
    df = spark.sql(sql)
    df.write.csv("./data1.csv",mode='overwrite',header=True)
    df.show()
    pass

def read_csv(spark):
    """
    header 是否第一行作为header
    schema 
    :param spark:
    :return:
    """

    schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("duration", IntegerType(), False),
        StructField("subject_1", IntegerType(), True),
        StructField("item_id", IntegerType(), True),
        StructField("create_time", IntegerType(), True)]
    )
    df = spark.read.csv('./data1.csv', header=True, schema=schema)
    # print(df.dtypes)
    # df.show()
    return df

def group_and_topn(spark,n=0):
    """
    以data.csv 为数据源，以item_id和subject_1分组取最新的create_time 前3ge
    :param spark:
    :param n:
    :return:
    """
    df=read_csv(spark)
    window = Window.partitionBy("user_id", 'item_id', 'subject_1').orderBy(df["create_time"].desc())
    df = df.withColumn('topn', F.row_number().over(window))
    df = df.where(df.topn <= 3)
    df.show()


def get_time(s):
    try:
        res = datetime.fromtimestamp(int(s)).strftime(("%Y-%m-%d"))
        return res
    except:
        return ''


time_udfs = udf(get_time, StringType())

# 使用
# df.select('id',time_udfs()).distinct()



if __name__ == '__main__':
    spark, ctx = create_spark('firstdemo')
    df=read_csv(spark)
    # df.show()
    df.select('user_id',time_udfs(df['create_time'])).distinct().show()
    # spark.stop()
    # group_and_topn(spark)
