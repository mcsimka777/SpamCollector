create external table volkov_prj_hcalls
(
	callid bigint,
	sdate text,
	stime text,
	ta text,
	tb text,
	tg int,
	rel int,
	duration int 
) location ('pxf://volkov.calls?PROFILE=hive&SERVER=hadoop')
format 'CUSTOM' (FORMATTER = 'pxfwritable_import');

create table volkov_prj_spammers 
(
	prefix text,
	tg int,
	days_spam bigint,
	primary key (prefix, tg)
);

create materialized view volkov_prj_hcalls_mv as
(
select 
	substring(ta, 1,4) as tag,
	count(ta) as calls, 
	extract(hour from stime::time) as hours,
	extract (day from to_date(sdate,'DD.MM.YYYY')) as day,
	to_date(sdate,'DD.MM.YYYY') as sdate, tg,
    count(case when duration>0 then 1 end) as full_call, count(case when duration=0 then 1 end) as empty_call,
    100*count(case when duration=0 then 1 end)/count(ta) as percent_unsuccessfull
from
	volkov_prj_hcalls 
where
	tg<>1000 and to_date(sdate,'DD.MM.YYYY') = to_date('23.09.2023','DD.MM.YYYY')
group by
  	tag,
	hours,
	day,
	sdate,
	tg
having 
  	count(ta)>30 and (100*count(case when duration=0 then 1 end)/count(ta)) > 80
)
distributed randomly;

create materialized view volkov_prj_week_chart_mv as
(
select 
  substring(ta, 1,4) as tag,
  count(ta) as calls,
  count(case when duration>0 then 1 end) as full_call,
  count(case when duration=0 then 1 end) as empty_call,
  100*count(case when duration=0 then 1 end)/count(ta) as percent_unsuccessfull
from volkov_prj_hcalls  
where tg<>1000 and to_date(sdate, 'DD.MM.YYYY') > to_date('21.09.2023', 'DD.MM.YYYY') - interval '7 day' 
group by
  tag
having 
  count(ta)>300 and (100*count(case when duration=0 then 1 end)/count(ta)) > 80
)
distributed randomly;

create materialized view volkov_prj_new_spam_mv  as
(
select 
	substring(ta, 1,4) as tag,
	count(ta) as calls, 
	extract(hour from stime::time) as hours,
	extract (day from to_date(sdate,'DD.MM.YYYY')) as day,
	to_date(sdate,'DD.MM.YYYY') as sdate, tg,
    count(case when duration>0 then 1 end) as full_call, count(case when duration=0 then 1 end) as empty_call,
    100*count(case when duration=0 then 1 end)/count(ta) as percent_unsuccessfull
from
	volkov_prj_hcalls 
where
	tg<>1000  and to_date(sdate,'DD.MM.YYYY') = to_date('23.09.2023','DD.MM.YYYY') 
group by
	tag,
	hours,
	day,
	sdate,
	tg
having 
  	count(ta)>30 and (100*count(case when duration=0 then 1 end)/count(ta)) > 80
  	and substring(ta, 1,4) not in (select prefix from volkov_prj_spammers)
)
distributed randomly;

create materialized view volkov_prj_spammers_mv as
(
	select * from volkov_prj_spammers where days_spam < 4
)
distributed randomly;


update volkov_prj_spammers vps set days_spam = vps.days_spam + 1 
from 
(
	select distinct substring(ta, 1,4) as tag, tg
	from volkov_prj_hcalls 
	where tg<>1000 and to_date(sdate,'DD.MM.YYYY') = to_date('22.09.2023','DD.MM.YYYY')
	group by tag, tg, extract(hour from stime::time)
	having count(ta)>30 and (100*count(case when duration=0 then 1 end)/count(ta)) > 80
) as subquery
where vps.prefix = subquery.tag and vps.tg = subquery.tg;

insert into volkov_prj_spammers (prefix, tg, days_spam)
select tag, tg, '1' as days_spam from
(
	select distinct substring(ta, 1,4) as tag, tg
 	from volkov_prj_hcalls 
	where tg<>1000 and to_date(sdate,'DD.MM.YYYY') = to_date('22.09.2023','DD.MM.YYYY')
	group by tag, tg, extract(hour from stime::time)
	having count(ta)>30 and (100*count(case when duration=0 then 1 end)/count(ta)) > 80
) as subquery 
where not exists (select 1 from volkov_prj_spammers sp where sp.prefix = subquery.tag and sp.tg = subquery.tg);
