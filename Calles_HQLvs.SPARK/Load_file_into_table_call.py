from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
df = spark.sql('use arstel')
df = spark.sql('drop table if exists arstel.mb_pir')
df = spark.sql('create table arstel.mb_pir \
         (numbers string, site string, text string) \
         row format delimited fields terminated by ","\
         lines terminated by "\n"\
         stored as textfile')    
df = spark.sql("load data local inpath '/export/home/mbabaeva/Investiciinumbersall.csv'\
                 overwrite into table arstel.mb_pir")
