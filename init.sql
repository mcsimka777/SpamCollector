create external table volkov_hcalls
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
format 'CUSTOM' (FORMATTER = 'pxfwritable_import'


create table volkov_prj_spammers 
(
	prefix text,
	tg int,
	days_spam bigint,
	primary key (prefix, tg)
);
