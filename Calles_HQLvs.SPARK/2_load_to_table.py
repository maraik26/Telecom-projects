from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

#---------------------------------------------Investicii

df = spark.sql('use arstel')
df = spark.sql('drop table if exists arstel.mb_investicii_numbers_all')
df = spark.sql('create table arstel.mb_investicii_numbers_all \
         (numbers string, site string, text string) \
         row format delimited fields terminated by ","\
         lines terminated by "\n"\
         stored as textfile')    
df = spark.sql("load data local inpath '/export/home/mbabaeva/SPARK/input_files3/Investiciinumbersall.csv'\
                 overwrite into table arstel.mb_investicii_numbers_all")

#------------------------------------------- Delivery_food

df0 = spark.sql('use arstel')
df0 = spark.sql('drop table if exists arstel.mb_delivery_food_numbers_all')
df0 = spark.sql('create table arstel.mb_delivery_food_numbers_all \
         (numbers string, site string, text string) \
         row format delimited fields terminated by ","\
         lines terminated by "\n"\
         stored as textfile')    

df0 = spark.sql("load data local inpath '/export/home/mbabaeva/SPARK/input_files3/deliveryfoodnumbersall.csv'\
                 overwrite into table arstel.mb_delivery_food_numbers_all")

