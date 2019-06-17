----------------------------------------------------------------COUNT
set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;
drop table if exists arstel.mb_march2019_clean55;
create table arstel.mb_march2019_clean55 as

select host, host_count from
(select lower(host) as host, count(lower(host)) as host_count from 
(SELECT * from spp.fct_sites_daily 
where substr(time_key,1,7) = "2019-03" and
lower(host) like "%.ru" and 
lower(host) not like "=%" and
lower(host) not like "'%" and
lower(host) not like "!%" and
lower(host) not like "=%" and
lower(host) not like "api%" and
lower(host) not like "*%" and
lower(host) not like "-%" and
lower(host) not like "+%" and
lower(host) not like "0-%" and
lower(host) not like "0%" and
lower(host) not like "%mts%" and
lower(host) not like "%megafon%" and
lower(host) not like "%cdnvideo%" and
lower(host) not like "%cdn%" and
lower(host) not like "%domolink%" and
lower(host) not like "%bashtel%" and
lower(host) not like "%api%" and
lower(host) not like "%server%" and
lower(host) not like "%tracking%" and
lower(host) not like "%google%" and
lower(host) not like "%instagram%" and
lower(host) not like "%yandex%" and
lower(host) not like "%mail%" and
lower(host) not like "%whatsapp%" and
lower(host) not like "%viber%" and
lower(host) not like "%beeline%" and
lower(host) not like "%counter%" and
lower(host) not like "%sync%" and
lower(host) not like "d-%" and
lower(host) not like "%footprint%" and
lower(host) not like "%.vk.%" and
lower(host) not like "%apple%" and
lower(host) not like "%android%" and
lower(host) not like "%twitter%" and
lower(host) not like "%porn%" and
lower(host) not like "%rambler%" and
lower(host) not like "%youtube%" and
lower(host) not like "%.telegram%" and
lower(host) not like "%.tellegram%" and
lower(host) not like "%skype%" and
lower(host) not like "%.ya.%" and
lower(host) not like "%.fb.%" and
lower(host) not like "%.ok.%" and
lower(host) not like "%.odnoklassniki.%" and
lower(host) not like "%rutube.ru%" and
lower(host) not like "%accuweather%")t
group by host) t
where host_count>300
order by host_count desc;
--------------------------------------------------------------------------------¡≈–®Ã “ŒÀ‹ Œ œ–¿¬»À‹Õ€≈ —¿…“€
set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;
drop table if exists arstel.mb_march2019_clean66;
create table arstel.mb_march2019_clean66 as
SELECT split(lower(host), '[.]') as list, host, host_count from arstel.mb_march2019_clean55
group by host, host_count;
----------------------------------------------------------------------------------------
set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;
drop table if exists arstel.mb_march2019_clean66_n;
create table arstel.mb_march2019_clean66_n as
SELECT count(*) as count, list, host, host_count from arstel.mb_march2019_clean66
LATERAL VIEW explode(list) t1 AS singleSegment
WHERE trim(singleSegment) != ""
GROUP BY list, host, host_count;

set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;
drop table if exists arstel.mb_march2019_clean77_n;
create table arstel.mb_march2019_clean77_n as
SELECT list, host, count, host_count from arstel.mb_march2019_clean66_n
where count=2
GROUP BY list, host,count, host_count;
-------------------------------------------------------------------------------------
set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;
drop table if exists arstel.mb_march2019_clean66_w;
create table arstel.mb_march2019_clean66_w as
SELECT count(*) as count, list, host, host_count from arstel.mb_march2019_clean66
LATERAL VIEW explode(list) t1 AS singleSegment
WHERE trim(singleSegment) != "" and singleSegment ='www'
GROUP BY list, host, host_count;

set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;
drop table if exists arstel.mb_march2019_clean77_w;
create table arstel.mb_march2019_clean77_w as
SELECT count(*) as count, list, host, host_count from arstel.mb_march2019_clean66_w
LATERAL VIEW explode(list) t1 AS singleSegment
WHERE trim(singleSegment) != ""
GROUP BY list, host, host_count;

set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;
drop table if exists arstel.mb_march2019_clean88_w;
create table arstel.mb_march2019_clean88_w as
SELECT list, host, count, host_count from arstel.mb_march2019_clean77_w
where count=3
GROUP BY list, host,count, host_count;
--------------------------------------------------------------------------------------------------------


set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;
drop table if exists arstel.mb_march2019_clean99;
create table arstel.mb_march2019_clean99 as
select t1.host, t1.host_count, t1.list
from
(
select host, host_count, list from arstel.mb_march2019_clean77_n
union
select host, host_count, list from arstel.mb_march2019_clean88_w
) t1;

set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;
drop table if exists arstel.mb_march2019_clean999;
create table arstel.mb_march2019_clean999 as
select * from arstel.mb_march2019_clean99
order by host_count desc;

------------------------------------------------------sites which are in both January and February ( —¿…“€ — ﬂÕ¬¿–ﬂ » ‘≈¬–¿Àﬂ- —”ÃÃ¿)
set mapreduce.job.queuename=adhoc;
set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;

drop table if exists arstel.mb_jan_feb2019_;
create table arstel.mb_jan_feb2019_ as
select t1.host, t1.host_count, t1.list
from
(
select host, host_count, list from arstel.mb_jan2019_clean999
union
select host, host_count, list from arstel.mb_feb2019_clean999
) t1;

drop table if exists arstel.mb_jan_feb2019;
create table arstel.mb_jan_feb2019 as
select * from arstel.mb_jan_feb2019_;

----------------------------------------------sites which are new only in march (—◊»“¿≈Ã —¿…“€,  Œ“Œ–€≈ œŒﬂ¬»À»—‹ “ŒÀ‹ Œ ¬ Ã¿–“≈ Ë Ëı ÌÂ ·˚ÎÓ ‚ ˇÌ‚‡Â Ë ÙÂ‚‡ÎÂ!)
set mapreduce.job.queuename=adhoc;
set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;

drop table if exists arstel.mb_march_unique2019;
create table mb_march_unique2019 as
select t1.host, t1.list from arstel.mb_march2019_clean999 t1
left join
(select host, list from arstel.mb_jan_feb2019)
topscore
on t1.host =  topscore.host
where topscore.host is NULL;
----------------------------------------------------
set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;
drop table if exists arstel.mb_march2019_unique_only_sites;
create table arstel.mb_march2019_unique_only_sites as
select distinct(host) from arstel.mb_march_unique2019;
