---------------- investicii
set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;

drop table if exists arstel.mb_call_union_investicii;
create table mb_call_union_investicii as
select t1.subs_key, t1.called_num, t2.site, t2.text from mb_test_call_union_full t1
join
(select numbers, site, text from mb_called_num_investicii)t2
on t1.called_num=t2.numbers;

-----------------------------------rent_property
set hive.execution.engine=tez;
set tez.queue.name=adhoc;
use arstel;

drop table if exists arstel.mb_call_union_rent_property;
create table mb_call_union_rent_property as
select t1.subs_key, t1.called_num, t2.site, t2.text from mb_test_call_union_full t1
join
(select numbers, site, text from mb_called_num_rent_property)t2
on t1.called_num=t2.numbers;
