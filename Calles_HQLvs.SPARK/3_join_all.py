from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

#------------------------------------------------------Investicii
df0 = spark.sql('use arstel')
df0 = spark.sql('drop table if exists arstel.mb_investicii_CALLES')

df0 = spark.sql('create table arstel.mb_investicii_CALLES as select t1.subs_key, t1.called_num, t2.site, t2.text from arstel.mb_pir_full t1\
	join (select numbers, site, text from arstel.mb_investicii_numbers_all)t2\
	on t1.called_num=t2.numbers')

#------------------------------------------------------Delivery_food
df1 = spark.sql('use arstel')
df1 = spark.sql('drop table if exists arstel.mb_delivery_food_CALLES')

df1 = spark.sql('create table arstel.mb_delivery_food_CALLES as select t1.subs_key, t1.called_num, t2.site, t2.text from arstel.mb_pir_full t1\
	join (select numbers, site, text from arstel.mb_delivery_food_numbers_all)t2\
	on t1.called_num=t2.numbers')

