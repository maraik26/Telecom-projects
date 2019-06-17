SET mapreduce.job.queuename=adhoc;
set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;

drop table if exists arstel.mb_called_num_investicii;
create table arstel.mb_called_num_investicii (numbers string, site string, text string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\,'
LINES TERMINATED BY '\n' 
STORED AS textFILE;
load data local inpath '/export/home/mbabaeva/Investiciinumbersall.csv'
overwrite into table arstel.mb_called_num_investicii;

