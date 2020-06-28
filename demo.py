import rlcompleter, readline
readline.parse_and_bind('tab: complete')

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col, lit, udf, to_date, collect_list, struct, desc, unix_timestamp, round
from pyspark.sql.types import *
from functools import reduce as rd 
from hdfs import InsecureClient


# 定義欄位名稱與型態
schema = StructType(
        [StructField("district", StringType(), True),
        StructField("transaction sign", StringType(), True),
        StructField("land sector position building sector house number plate", StringType(), True),
        StructField("land shifting total area square meter", StringType(), True),
        StructField("the use zoning or compiles and checks", StringType(), True),
        StructField("the non-metropolis land use district", StringType(), True),
        StructField("non-metropolis land use", StringType(), True),
        StructField("transaction date", StringType(), True),
        StructField("transaction pen number", StringType(), True),
        StructField("shifting level", StringType(), True),
        StructField("total floor number", StringType(), True),
        StructField("building state", StringType(), True),
        StructField("main use", StringType(), True),
        StructField("main building materials", StringType(), True),
        StructField("construction to complete the years", StringType(), True),
        StructField("building shifting total area", StringType(), True),
        StructField("Building present situation pattern - room", StringType(), True),
        StructField("building present situation pattern - hall", StringType(), True),
        StructField("building present situation pattern - health", StringType(), True),
        StructField("building present situation pattern - compartmented", StringType(), True),
        StructField("Whether there is manages the organization", StringType(), True),
        StructField("total price NTD", StringType(), True),
        StructField("the unit price (NTD / square meter)", StringType(), True),
        StructField("the berth category", StringType(), True),
        StructField("berth shifting total area square meter", StringType(), True),
        StructField("the berth total price NTD", StringType(), True),
        StructField("the note", StringType(), True),
        StructField("serial number", StringType(), True)])


# 設定檔名與城市
city_config = {
        'A_lvr_land_A.csv': '台北市',
        'B_lvr_land_A.csv': '台中市',
        'E_lvr_land_A.csv': '高雄市',
        'F_lvr_land_A.csv': '新北市',
        'H_lvr_land_A.csv': '桃園市'
        }


# spark寫入elasticsearch設定
es_write_conf = {
        'es.nodes': 'localhost', 
        'es.port': '9200',
        'es.resource': 'interview/Cathy',
        'es.input.json': 'yes',
        'es.mapping.id': 'serial number'}


# 讀取資料夾路徑 hdfs
InputDir = '/user/DemoData/'


# 輸出資料夾路徑 hdfs
OutputDir = '/tmp/Cathay/'


# 讀取路徑資料夾下所有檔案，並新增城市資料，最後合併所有df
def ReadData(dirPath, city_config, schema, spark):
    dfs = []
    for fn, city in city_config.items():
        fp = dirPath + fn
        df_temp = spark.read\
            .option('encoding', 'UTF-8')\
            .schema(schema)\
            .csv(fp)
        df_temp = df_temp.withColumn("index", monotonically_increasing_id()).filter(col('index')>1).drop('index')
        df_temp = df_temp.withColumn('city', lit(city))
        dfs.append(df_temp)
    df = rd(DataFrame.unionAll, dfs)
    return df


# 將民國轉為西元
def convert_date(x):
    if (x != None):
        x = x.strip()
        if ((x != '') & ((len(x) > 4) & (len(x) < 8)) ):
            month = x[-4:-2]
            day = x[-2:]
            if ( ((int(month) <= 12) & (int(month) > 0)) & ((int(day) < 32) & (int(day) > 0)) ):
                year = str(int(x[:-4]) + 1911)
                return year + month + day
            else:
                return None
        else:
            return None
    else:
        return None


def find_buildingType(x):
    if (x != None):
        building_type = x.strip().split('(')[0]
        return building_type
    else:
        return None




# 將rdd資料型態轉為Elasticsearch支援格式
def format_data(x):
    return (eval(x)['serial number'], x)


# (第二題) 將輸出的檔案名稱由part-00000 -> result-part1.json
def renameFiles(ip='172.20.10.2', port='9870', username='jw0908', MainName='result-part', SubName='.json', dirPath='/tmp/Cathay/'):
    client = InsecureClient("http://" + ip  + ":" + port, user=username)
    if dirPath[-1] != '/':
        dirPath += '/'
    fns = client.list(dirPath)
    for fn in fns:
        if 'part-' in fn:
            num = str(int(fn.split('part-')[-1])+1)
            client.rename(dirPath+fn, dirPath+MainName+num+SubName)
    return str(fns) + "\n     Change to     \n" + str(client.list(dirPath))



def main():
    
    spark = SparkSession\
            .builder\
            .appName("demo")\
            .getOrCreate()
    
    # 讀取目標資料夾下所有city_config中的檔案，並給予相對應城市欄位
    df = ReadData(InputDir, city_config, schema, spark)
    
    # 透過UDF 將民國(string) 轉為 西元(date)
    udf_convert_date = udf(convert_date, StringType())
    df = df.withColumn('transaction date', to_date(udf_convert_date('transaction date'), 'yyyyMMdd'))
    df = df.withColumn('construction to complete the years', to_date(udf_convert_date('construction to complete the years'), 'yyyyMMdd'))
    # df = df.filter(col('transaction date') >= '2018-12-21').filter(col('transaction date') <= '2018-12-31')
    # 處理數值欄位 string -> Integer & Float
    int_cols = ["Building present situation pattern - room", "Building present situation pattern - hall",\
                "Building present situation pattern - health", "total price NTD", "the unit price (NTD / square meter)"]
    float_cols = ["land shifting total area square meter", "building shifting total area", "berth shifting total area square meter"]
    for col_name in int_cols:
        df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
    for col_name in float_cols:
        df = df.withColumn(col_name, col(col_name).cast(FloatType()))

    df_cached = df.cache()

# ====================================================================================================================

# ================================================ 第一題 ============================================================

# ====================================================================================================================
#   新增屋齡
    df1 = df_cached.withColumn('house age', round( 
        (unix_timestamp('transaction date', 'yyyy-MM-dd')-unix_timestamp('construction to complete the years', 'yyyy-MM-dd'))/ 31536000, 2))
    
#   房屋類別    
    udf_find_buildingType = udf(find_buildingType, StringType())
    df1 = df1.withColumn('building type', udf_find_buildingType('building state'))

#   將資料寫入Elasticsearch
#   將資料轉成rdd json格式
    esData = df1.toJSON()


#   整理成elasticsearch支援格式
#   '{'city':'台北市', transaction date: .....}'
#   轉成
#   ('serial number': xxxx, '{'city':'台北市', transaction date: .....}')
    esData = esData.map(lambda x: format_data(x))
#   將資料寫入本地端Elasticsearch 
    esData.saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=es_write_conf)

# ====================================================================================================================

# ================================================ 第二題 ============================================================

# ====================================================================================================================
#   將資料整成 {city: 台北市, time_slots: [{date: 2020-01-01, events: [{district: 文山區, type: 其他}, ...]} ...]}
#   使用collect_list和struct 將欄位弄成 nested column
#   重新命名成題目樣式
    df2 = df_cached.withColumnRenamed('building state', 'type')\
                   .withColumnRenamed('transaction date', 'date')


#   先 groupby 城市和日期已建立題目內events內容
#   並對城市和交易日期做排序
    df2 = df2.groupby('city', 'date').agg(collect_list(struct(col('type'), col('district'))).alias('events'))
    df2 = df2.sort('city', desc('date'))
#   ======================================================
#   +------+----------+---------------------------+                                 
#   |  city|      date|                     events|
#   +------+----------+---------------------------+
#   |台中市|2019-01-30|           [[其他, 大甲區]]|
#   |台中市|2019-01-27|         [[透天厝, 太平區]]|
#   |台中市|2019-01-25|           [[其他, 新社區]]|
#   |台中市|2019-01-24|           [[其他, 梧棲區]]|
#   |台中市|2019-01-23|[[其他, 大安區], [其他, ...|
#   +------+----------+---------------------------+
#   |-- city: string (nullable = false)
#   |-- date: date (nullable = true)
#   |-- events: array (nullable = true)
#   |    |-- element: struct (containsNull = true)
#   |    |    |-- type: string (nullable = true)
#   |    |    |-- district: string (nullable = true)
#   ======================================================


#   再groupby 城市 以建立題目內time_slots 內容
    df2 = df2.groupby('city').agg(collect_list(struct(col('date'), col('events'))).alias('time_slots'))
#   ======================================================
#   +------+---------------------+                                                  
#   |  city|           time_slots|
#   +------+---------------------+
#   |台北市|[[2019-04-01, [[住...|
#   |新北市|[[2019-01-31, [[住...|
#   |台中市|[[2019-01-30, [[其...|
#   |高雄市|[[2019-01-31, [[店...|
#   |桃園市|[[2019-01-28, [[其...|
#   +------+---------------------+
#   |-- city: string (nullable = false)
#   |-- time_slots: array (nullable = true)
#   |    |-- element: struct (containsNull = true)
#   |    |    |-- date: date (nullable = true)
#   |    |    |-- events: array (nullable = true)
#   |    |    |    |-- element: struct (containsNull = true)
#   |    |    |    |    |-- type: string (nullable = true)
#   |    |    |    |    |-- district: string (nullable = true)
#   ======================================================


#   減少分區至2 ，以達到5座城市資料隨機分佈在2個檔案之中(part-00000, part-00001)
    data = df2.toJSON()
    data.coalesce(2, True).saveAsTextFile(OutputDir)

#   重新命名為題目要求 result-part1.json ...
    result = renameFiles()
    print(result)
    spark.stop()


if __name__ == '__main__':
    main()
