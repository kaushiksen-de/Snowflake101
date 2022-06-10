--Create a table

create table user_details_staging
(user_id int,name string,city string,country string);
      
--create the target table

create table user_details_target
(user_id int,name string,city string,country string);

--create stream

create or replace stream user_details_stream
on table user_details_staging;

--check stream (0 rows)

select * from user_details_stream;

--insert some values

insert into user_details_staging
values(1,'Kaushik','Chennai','India'),
      (2,'Kanthini','Los Angeles','USA'),
      (3,'Cheemu','Toronto','Canada');

--check stream now (3 rows)

select * from user_details_stream;

--create task and merge the data to the target

CREATE OR REPLACE  task poc2_task 
warehouse = compute_wh
schedule = '1 MINUTE'
when SYSTEM$STREAM_HAS_DATA('user_details_stream')
AS
merge into user_details_target target
using user_details_stream input
on input.user_id = target.user_id 
when matched  
and input.METADATA$ACTION = 'INSERT'
and input.METADATA$ISUPDATE = true
then update
set
target.name = input.name,
target.city = input.city,
target.country = input.country
when matched 
and input.METADATA$ACTION = 'DELETE'
and input.METADATA$ISUPDATE = false
then delete
when not matched
and input.METADATA$ACTION = 'INSERT' 
then 
insert (user_id,name,city,country) 
values(input.user_id,input.name,input.city,input.country);

--resume task

alter task poc2_task resume;

--check target table

select * from user_details_target;

--check task history

select * from table (information_schema.task_history(
scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
result_limit=>100,
task_name=>'poc2_task'));

--add a record, update a record and delete a record

insert into user_details_staging 
values (4,'Paul','New York','America');
update user_details_staging 
set city = 'San Diego'
where user_id = 2;
delete from user_details_staging where user_id = 3;

--check target table

select * from user_details_target;