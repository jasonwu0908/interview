import rlcompleter, readline
readline.parse_and_bind('tab: complete')

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col, lit, udf, to_date, collect_list, struct, desc
from pyspark.sql.types import *
from functools import reduce as rd 
from hdfs import InsecureClient

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


city_config = {
        'A_lvr_land_A.csv': '台北市',
        'B_lvr_land_A.csv': '台中市',
        'E_lvr_land_A.csv': '高雄市',
        'F_lvr_land_A.csv': '新北市',
        'H_lvr_land_A.csv': '桃園市'
        }

es_write_conf = {
        'es.nodes': 'localhost', 
        'es.port': '9200',
        'es.resource': 'interview/Cathy',
        'es.input.json': 'yes',
        'es.mapping.id': 'serial number'}


InputDir = '/user/DemoData/'
OutputDir = '/tmp/Cathay/'


def ReadData(dirPath, city_config, schema):
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



def convert_date(x):
    if x != None:
        if ((x != '') & (len(x)>4)):
            x = x.strip()
            month = x[-4:-2]
            if ((int(month) <= 12) & (int(month) > 0)):
                year = str(int(x[:-4]) + 1911)
                day = x[-2:]
                return year + month + day
            else:
                return None
        else:
            return None
    else:
        return None



def format_data(x):
    return (eval(x)['serial number'], x)



def renameFiles(ip='10.61.100.134', port='9870', username='jw0908', MainName='result-part', SubName='.json', dirPath='/tmp/Cathay/'):
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
            .appName("demo1")\
            .getOrCreate()
    # 讀取目標資料夾下所有city_config中的檔案，並給予相對應城市欄位
    df = ReadData(InputDir, city_config, schema)
    # 處理時間欄位
    udf_convert_date = udf(convert_date, StringType())
    df = df.withColumn('transaction date', to_date(udf_convert_date('transaction date'), 'yyyyMMdd'))
    df = df.withColumn('construction to complete the years', to_date(udf_convert_date('construction to complete the years'), 'yyyyMMdd'))
    # 處理數值欄位
    int_cols = ["Building present situation pattern - room", "Building present situation pattern - hall",\
                "Building present situation pattern - health", "total price NTD", "the unit price (NTD / square meter)"]
    float_cols = ["land shifting total area square meter", "building shifting total area", "berth shifting total area square meter"]
    for col_name in int_cols:
        df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
    for col_name in float_cols:
        df = df.withColumn(col_name, col(col_name).cast(FloatType()))
# ====================================================================================================================

# ================================================ 第一題 ============================================================

# ====================================================================================================================

#    將資料寫入Elasticsearch

#   將資料轉成rdd json格式
    esData = df.toJSON()
    esData = esData.map(lambda x: format_data(x))
    esData.saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=es_write_conf)


# ====================================================================================================================

# ================================================ 第二題 ============================================================

# ====================================================================================================================

    # 將資料整成 {city: 台北市, time_slots: [{date: 2020-01-01, events: [{district: 文山區, type: 其他}, ...]} ...]}

    df2 = df.withColumnRenamed('building state', 'type')\
            .withColumnRenamed('transaction date', 'date')
    df2 = df2.groupby('city', 'date').agg(collect_list(struct(col('type'), col('district'))).alias('events'))
    df2 = df2.sort('city', desc('date'))
    df2 = df2.groupby('city').agg(collect_list(struct(col('date'), col('events'))).alias('time_slots'))
    data = df2.toJSON()
    data.coalesce(2, True).saveAsTextFile(OutputDir)
    result = renameFiles()
    print(result)
    
    sc.stop()




if __name__ == '__main__':
    main()
