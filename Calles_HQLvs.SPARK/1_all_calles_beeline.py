from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

df1 = spark.sql('use arstel')
df1 = spark.sql('drop table if exists arstel.mb_pir_full')

df1 = spark.sql('create table arstel.mb_pir_full as select distinct r.subs_key, r.CALLED_NUM from (select CALLED_NUM, SUBS_KEY, CALL_START_TIME from BIIS_FCT.FCT_USAGE_PREP_CHA_N where \
          (CALL_START_TIME>="P20181001" and CALL_START_TIME<="P20190324" and CALL_TYPE_CODE="V" and \
                 CALL_DIRECTION_IND="2" and actual_call_duration_sec>10) \
                 union \
                 select CALLED_NUM, SUBS_KEY, t4.CALL_START_TIME from  BIIS_FCT.FCT_USAGE_POSTPAID t4 where \
	(CALL_START_TIME>="P20181001" and CALL_START_TIME<="P20190324" and CALL_TYPE_CODE="V" and \
                 CALL_DIRECTION_IND="2" and actual_call_duration_sec>10)) r')