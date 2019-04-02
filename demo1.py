from pyspark.sql import SparkSession, SQLContext
import os

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, ArrayType,DataType

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






schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("duration", StringType(), False),
    StructField("subject_1", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("create_time", StringType(), True) ]
)

# str1=StructType([StructField('user_id'),IntegerType(),True])
def read_csv(spark):
    df = spark.read.csv('./data1.csv', header=True,schema=schema)
    print(df.dtypes)
    df.show()
if __name__ == '__main__':
    spark, ctx = create_spark('firstdemo')
    #
    # register_db(ctx=ctx, url=con['url'],table=con['table'], password=con['password'], user=con['user'], registertable='mytable')
    # sql = '''select user_id,duration,subject_1,item_id,create_time from mytable '''
    # df = spark.sql(sql)
    # df.write.csv("./data1.csv",mode='overwrite',header=True)
    # df.show()
    read_csv(spark)
    spark.stop()
