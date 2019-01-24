public.event (holds the RAW data from the challenge)
public.dim_person 
public.dim_connection 
public.dim_screen
public.dim_action
public.dim_screen_action

-- Build table for raw data provided by Ben
CREATE TABLE public.event(
		event_timestamp timestamp,
		player_id varchar(200),
		subject_id varchar(200),
		rating_type integer)

-- Load data provided by Ben into [public.event] 
copy public.event from 's3://hinge-homework/director-data-engineering/ratings/' credentials 'aws_access_key_id=xxxxx;
aws_secret_access_key=xxxxx' delimiter '\t' IGNOREHEADER 0 ACCEPTINVCHARS as '?' ENCODING 
AS UTF8 TRUNCATECOLUMNS ACCEPTANYDATE dateformat 'auto';
COMMIT;

-- Create person dimension table
CREATE TABLE public.dim_person(
        person_key bigint IDENTITY(1,1),
        person_hash varchar(255),
        etl_hash_text nvarchar(35),
        created_timestamp timestamp  ,
        updated_timestamp timestamp);

-- Load data into [dim_person] 
insert into public.dim_person (person_hash)
select distinct player_id from public.event
union
select distinct subject_id from public.event

-- Build table for connections
CREATE TABLE public.dim_connection(
        connection_key integer IDENTITY(1,1),
        player_id integer not null,
        subject_id integer not null,
        etl_hash_text nvarchar(35),
        created_timestamp timestamp
);

-- Load data into [dim_connection]
insert into public.dim_connection (player_id,subject_id)
select p.person_key as player_id,p2.person_key as subject_id from public.event e 
inner join public.dim_person p on p.person_hash =  e.player_id
inner join public.dim_person p2 on p2.person_hash = e.subject_id
UNION
select p.person_key as player_id,p2.person_key as subject_id from public.event e 
inner join public.dim_person p on p.person_hash =  e.subject_id
inner join public.dim_person p2 on p2.person_hash = e.player_id


-- Create lookup table for screens
CREATE TABLE public.dim_screen(
		screen_key integer, 
		screen_name varchar(200), 
		screen_description varchar(500),
		etl_hash_text nvarchar(35),
		created_timestamp timestamp,
		updated_timestamp timestamp
 );

-- Load data into [dim_screen]
insert into dim_screen (screen_key,screen_name,screen_description,etl_hash_text,created_timestamp,updated_timestamp)
values (1,'Discover','Where a player can see a queue (one at the time) of subject profiles for the first time','-',getdate(),getdate());
insert into dim_screen (screen_key,screen_name,screen_description,etl_hash_text,created_timestamp,updated_timestamp)
values (2,'Likes You','where the player can review all of the people who have sent likes to them','-',getdate(),getdate());
insert into dim_screen (screen_key,screen_name,screen_description,etl_hash_text,created_timestamp,updated_timestamp)
values (3,'Chat','where the player can chat with people with whom she has matched','-',getdate(),getdate());

-- Create lookup table for actions
CREATE TABLE public.dim_action(
		action_key integer, 
		action_name varchar(50),
		action_value integer, 		
		etl_hash_text nvarchar(35),
		created_timestamp timestamp,
		updated_timestamp timestamp
 ); 

-- Load data into [dim_action] 
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (1,'Skip',0,'-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (2,'Like',1,'-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (3,'Comment',2,'-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (4,'Remove',3,'-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (5,'Block',3,'-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (6,'Reject',3,'-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (7,'Report',4,'-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (8,'Match',5,'-',getdate(),getdate());

-- Create M:N table for [dim_screen_action] 
CREATE TABLE public.dim_screen_action(
		screen_action_key integer, 
		screen_key integer, 
		action_key integer,
        screen_action_meaning varchar(50),
        etl_hash_text nvarchar(35),
        created_timestamp timestamp  ,
        updated_timestamp timestamp
        );

-- Load data into [dim_screen_action] 
insert into dim_screen_action (screen_action_key,screen_key,action_key,screen_action_meaning,etl_hash_text,created_timestamp,updated_timestamp)
values (1,1,1,'Skip on Discover','-',getdate(),getdate());
insert into dim_screen_action (screen_action_key,screen_key,action_key,screen_action_meaning,etl_hash_text,created_timestamp,updated_timestamp)
values (2,1,2,'Like on Discover','-',getdate(),getdate());
insert into dim_screen_action (screen_action_key,screen_key,action_key,screen_action_meaning,etl_hash_text,created_timestamp,updated_timestamp)
values (3,1,3,'Like w/Comment on Discover','-',getdate(),getdate());
insert into dim_screen_action(action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (4,1,7,'Report on Discover','-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (5,1,4,'Remove on Discover','-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (6,2,6,'Reject on Likes You','-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (7,2,7,'Report on Likes You','-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (8,2,8,'Match on Likes You','-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (9,3,5,'Block on Chats','-',getdate(),getdate());
insert into dim_action (action_key,action_name,action_value,etl_hash_text,created_timestamp,updated_timestamp)
values (10,3,7,'Report on Chats','-',getdate(),getdate());

-- Create a Staging table which processes the RAW event data [public.event].  This table will be used to 
-- Transform the data for the FACT table.  This table is needed in order to understand the order of communication
-- And figure out the content of the rating_type, since the value 3 can have mulitple semantics on what order it 
-- arrives. Us

CREATE TABLE public.event_staging (
		event_timestamp timestamp, 
		player_id varchar(200), 
		subject_id varchar(200), 
		rating_type integer, 
		person_key integer, 
		subject_key integer, 
		connection_nbr integer, 
		least_key integer)

-- Load staging table 
insert into public.event_staging
select e.event_timestamp, e.player_id, e.subject_id,e.rating_type, p.person_key,r.person_key, 
sum(p.person_key + r.person_key) as connection_nbr,
least(p.person_key,r.person_key)
  from public.event e, 
       public.dim_person p,
	   public.dim_person r
 where p.person_hash = e.player_id
   and r.person_hash = e.subject_id
group by 1,2,3,4,5,6

-- Create BASE FACT Table [fact_event]
CREATE TABLE public.dim_person(
        event_key bigint IDENTITY(1,1),
        connection_key integer,
        screen_action_key integer,
        event_timestamp timestamp
        );



