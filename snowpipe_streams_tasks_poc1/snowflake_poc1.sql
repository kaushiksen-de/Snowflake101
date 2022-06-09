--Create Database
create database snowflake_poc1;
use database snowflake_poc1;

--Create Integration Object
create or replace storage integration s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN='arn:aws:iam::675793657996:role/snowflake-S3-role'
STORAGE_ALLOWED_LOCATIONS=('s3://snowflakebuckpoc1/input1');

--See properties of Integration Object and take the external_id
DESC INTEGRATION s3_int;

--Create stage table
CREATE or REPLACE table user_data_input
(id int,
first_name string,
last_name string,
email_id string,
city string,
country string);

--Create File Format Object

Create or replace file format file_format_snowflakepoc1_input
type = csv
field_delimiter = ','
skip_header = 1
null_if = ('NULL','null')
empty_field_as_null = true
field_optionally_enclosed_by ='"';

--check the file format properties
desc file format file_format_snowflakepoc1_input;

--Create stage object using our integration and file format objects
CREATE or REPLACE stage external_stages_snowflakepoc1
URL = 's3://snowflakebuckpoc1/input1'
STORAGE_INTEGRATION=s3_int
FILE_FORMAT=file_format_snowflakepoc1_input;

--Check the stage properties
DESC stage external_stages_snowflakepoc1; 

--Load data into our table from s3

COPY INTO user_data_input
FROM @external_stages_snowflakepoc1;

--Display the records
select * from user_data_input;

--list items in the stage
list @external_stages_snowflakepoc1;

--create snowpipe to automate load from s3 to snowflake
CREATE or REPLACE pipe snowflakepoc1_pipe
auto_ingest = TRUE
AS 
COPY INTO user_data_input
FROM @external_stages_snowflakepoc1;

--see the properties of the pipe and get the notification_channel value
DESC pipe snowflakepoc1_pipe;

--Validate data after loading two files (snowpipe 30-60 secs files loaded)
select * from user_data_input
order by id asc;

--Refresh pipe
alter pipe snowflakepoc1_pipe refresh;

--Check if Pipe is running
select SYSTEM$PIPE_STATUS('snowflakepoc1_pipe');

--Snowpipe error message if any
select * from TABLE(VALIDATE_PIPE_LOAD(
PIPE_NAME => 'snowflakepoc1_pipe',
start_time => dateadd(hour,-2,current_timestamp())));

--copy history error message if any
select * from table(information_schema.copy_history(
table_name => 'user_data_input',
start_time => dateadd(hour,-2,current_timestamp())));

--show pipes

show pipes;

--pause the execution of the pipe

alter pipe snowflakepoc1_pipe set pipe_execution_paused = true; --false to resume

--create a target table

CREATE or REPLACE table user_data_target
(id int,
first_name string,
last_name string,
email_id string,
city string,
country string);

--Create a stream

CREATE OR REPLACE stream user_data_stream on table user_data_input;

DESC stream user_data_stream;

--Create Task

CREATE OR REPLACE  task poc1_task 
warehouse = compute_wh
schedule = '1 MINUTE'
when SYSTEM$STREAM_HAS_DATA('user_data_stream')
AS
merge into user_data_target target
using user_data_stream input
on input.id = target.id 
when matched then update 
set
target.first_name = input.first_name,
target.last_name = input.last_name,
target.email_id = input.email_id,
target.city = input.city,
target.country = input.country
when not matched then 
insert (id,first_name,last_name,email_id,city,country) 
values(input.id,input.first_name,input.last_name,input.email_id,input.city,input.country);

DESC task poc1_task;

--insert initial data which is already in staging.

insert into user_data_target
select * from user_data_input;

--Resume Task

alter task poc1_task resume;

--Now Stage a new file in S3.

list @external_stages_snowflakepoc1;

select * from user_data_input order by id desc;

select count(*) from user_data_target; 

--check task history

select * from table (information_schema.task_history(
scheduled_time_range_start=>dateadd('hour',-0.01,current_timestamp()),
result_limit=>100,
task_name=>'poc1_task'));

--check stream

select * from user_data_stream;

--check if update works

select * from user_data_target where id = 1041; --Deloria Leonard

--post an updated file for id = 1041 with name change and see the data

select * from user_data_target where id = 1041;













