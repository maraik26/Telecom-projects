set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;

create table mb_test_call_union_full as
select distinct r.subs_key, r.CALLED_NUM
from 
(
select CALLED_NUM, SUBS_KEY, CALL_START_TIME
from BIIS_FCT.FCT_USAGE_PREP_CHA_N
where 
(
CALL_START_TIME>='P20181101' and CALL_START_TIME<='P20190130' and 
CALL_TYPE_CODE='V' and
CALL_DIRECTION_IND='2' and
actual_call_duration_sec>10
)
union
select CALLED_NUM, SUBS_KEY, t4.CALL_START_TIME 
from  BIIS_FCT.FCT_USAGE_POSTPAID t4	
where 
(
CALL_START_TIME>='P20181101' and CALL_START_TIME<='P20190130' and 
CALL_TYPE_CODE='V' and
CALL_DIRECTION_IND='2' and
actual_call_duration_sec>10
)
) r