--#redshift
--用户group_id
create temp table temp_user_group as
select distinct user_id,(case when group_id is null then '-1' else group_id end) group_id,app_name,platform,
first_value(ts ignore nulls) over (partition by user_id,app_name,platform,(case when group_id is null then '-1' else group_id end) order by ts,level
rows between unbounded preceding and unbounded following) as first_ts
from raw_data_wordview.play_level
where app_name = 'word_view'
and game_version in ('1.0.18','1.0.21')
;

create temp table temp_user_info as
select app_name,platform,user_id,
listagg(group_id::varchar,'-') within group (order by first_ts) as group_id,
listagg(first_ts::varchar,',') within group (order by first_ts) as ts,
count(group_id) group_ids
from temp_user_group
group by 1,2,3
;

create temp table temp_result as
select * from $temp_user_info_t where group_id = '2-0-1' limit 1000;

select * from temp_result;
--redshift#

--#redshift
select * from raw_data_word.play_levels limit 11;
select * from raw_data_word.game_starts limit 11;
--redshift#

--#firebase
create table temp.test as
select * from fact_wordview.new_users_info limit 10;

select * from temp.test limit 1;
--firebase#

--#firebase
select * from temp.test limit 2;
select * from temp.test limit 3;

--firebase#

--#firebase
--2018年9月12日

--firebase#
