from pyspark.sql import SparkSession
import os

url = 'spark://master:7077'
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
        .enableHiveSupport() \
        .master(url) \
        .appName(name) \
        .getOrCreate()
    ctx = spark.sparkContext(spark)
    return spark, ctx


def conf_spark(spark):
    """
    配置spark
    :return:
    """
    spark.conf.set("spark.executor.memmory", "500M")
    spark.conf.set("spark.cores.max", "2")
    spark.conf.set("spark.driver.maxResultSize", "2g")


def read_db(ctx, table, user, url, password):
    """
    连接mysql数据库,注册table
    :return:
    """
    driver = 'com.mysql.cj.jdbc.Driver'

    userdf = ctx.read.format("jdbc").options(url=url, driver=driver, dbtable=table,
                                             user=user, password=password).load()

    userdf.createOrReplaceTempView("mytable")


def create_df_from_sql(spark):
    """
    数据库读取数据产生df
    :param spark:
    :return:
    """
    sql = '''select user_id,duration,subject_1,item_id,create_time  from table_0 where channel=1 and user_id in (select user_id from usertable where if_delete=0) '''

    dbdf= spark.sql(sql)
    dbdf.show()
