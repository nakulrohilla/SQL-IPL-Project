-- Create a table named 'deliviries' with appropriate data types for columns.

create table deliveries (
    id int,
    inning int,
    over int,
    ball int,
    batsman char(100),
    non_striker char(100),
    bowler char(100),
    batsman_runs int,
    extra_runs int,
    total_runs int,
    is_wicket int,
    dismissal_kind char(50),
    player_dismissed char(100),
    fielder char(100),
    extras_type char(25),
    batting_team char(50),
    bowling_team char(50)	)
	
--drop table deliveries;
select * from deliveries;


-- Create a table named 'matches' with appropriate data types for columns.

create table matches(
 
	id int primary key,
	city char(50),
	date date,
	player_of_match char(100),
	venue char(100),
	neutral_venue int,
	team_1 char(50),
	team_2 char(50),
	toss_winner char(50),
	toss_decision char(50),
	winner char(50),
	result char(50),
	result_margin int,
	eliminator char(5),
	method varchar(5),
	umpire_1 char(50),
	umpire_2 char(50))
	
--drop table matches;
select * from matches;


-- Import data from csv file'IPL_matches.csv' attached in resources to 'matches'.
copy matches from 'C:\Program Files\PostgreSQL\15\data\Data for final Project IPLMatches & IPLBall\IPLMatches+IPLBall\IPL_matches.csv' csv header;

-- Import data from csv file'IPL_ball.csv' attached in resources to 'deliveries'.
copy deliveries from 'C:\Program Files\PostgreSQL\15\data\Data for final Project IPLMatches & IPLBall\IPLMatches+IPLBall\IPL_Ball.csv' csv header;

-- Select the top 20 rows of the matches table.
select * from matches order by id limit 20;

-- Fetch data of all the matches played on 2nd may 2013.
select * from matches where date = '02-05-2013';

-- Fetch data of all the matches where the margin of victory is more than 100 runs.
select * from matches where result = 'runs' and result_margin>'100'
order by result_margin desc;

-- Fetch data of all the matches where the final scores of both teams tied and order it in descending order of the date.
select * from matches where result = 'tie'
order by date desc;

-- Get the count of cities that have hosted an IPL matches.
select count(distinct city) as "Total_City_Hosted" from matches;

-- Create table deliveries_v02 with all the columns of deliveries and an additional column ball_result containing value boundary, dot or other depending on the total_run (boundary for >= 4, dot for 0 and other for -- any other number)  
create table deliveries_v02 as select *,
CASE WHEN total_runs >= 4 then 'boundary' 
WHEN total_runs = 0 THEN 'dot' else 'other' 
END as ball_result 
FROM deliveries; 
 
select * from deliveries_v02; 
-- drop table deliveries_v02; 

-- Write a query to fetch the total number of boundaries and dot balls.
select ball_result, count (*) from deliveries_v02 where ball_result <>'1,2,3' group by ball_result order by count desc; 
select ball_result, count (*) from deliveries_v02 group by ball_result; 
 
-- Write a query to fetch the total number of boundaries scored by each team .
select batting_team,count(*) from deliveries_v02 where ball_result = 'boundary' group by batting_team order by count; 

--Write a query to fetch the total number of dot balls bowled by each team.
select bowling_team, count(*) from deliveries_v02 where ball_result = 'dot' group by bowling_team order by count; 
 
-- Write a query to fetch the total number of dismissals by dismissal kinds.
select dismissal_kind, count (*) from deliveries where dismissal_kind <> 'NA' group by dismissal_kind order by count desc; 
 
-- Write a query to get the top 5 bowlers who conceded maximum extra runs. 
select distinct "bowler" as Bowler, count (extra_runs) as "Max_extra_runs" from deliveries group by bowler 
order by "Max_extra_runs" desc limit 5; 
-- select bowler, count (extra_runs) from deliveries group by bowler 
-- order by count desc limit 5; 


-- Write a query to create a table named deliveries_v03 with all the columns of deliveries_v02 table and  two additional column (named venue and match_date) of venue and date from table matches.
create table deliveries_v03 AS SELECT a.*, b.venue, b.match_date from deliveries_v02 as a left join (select max(venue) as venue, max(date) as match_date, id from matches group by id) as b on a.id = b.id; 
-- select * from deliveries_v03; 
 
-- Write a query to fetch the total runs scored for each venue and order it in the descending order of total runs scored. 
select sum (total_runs) as Venue_runs,(select venue from matches where deliveries.id = matches.id) from deliveries group by venue order by "venue_runs" desc; 
-- select venue, sum(total_runs) as runs from deliveries_v03 group by venue order by runs desc; 
 

-- Write a query to fetch the year-wise total runs scored at Eden Gardens and order it in the descending order of total runs scored. 
select extract(year from match_date) as IPL_year, sum(total_runs) as runs from deliveries_v03 where venue = 'Eden Gardens' group by IPL_year order by runs desc; 
 
-- Get unique team1 names from the matches table, you will notice that there are two entries for Rising Pune Supergiant one with Rising Pune Supergiant and another one with Rising Pune Supergiants. Your task is to create a matches_corrected table with two additional columns team1_corr and team2_corr containing team names with replacing Rising Pune Supergiants with Rising Pune Supergiant. Now analyse these newly created columns.
select distinct team_1 from matches; 
create table matches_corrected as select *, replace(team_1, 'Rising Pune Supergiants', 'Rising Pune Supergiant') as team1_corr ,
replace(team_2, 'Rising Pune Supergiants', 'Rising Pune Supergiant') as team2_corr from matches; 
select distinct team1_corr from matches_corrected; 
 
-- Create a new table deliveries_v04 with the first column as ball_id containing information of match_id, inning, over and ball separated by 8-9 (For ex. 335982-1-0-1 match_id-inning-over-ball) and rest of the columns same as deliveries_v03). 
create table deliveries_v04 AS SELECT concat(id,'-',inning,'-',over,'-',ball) as ball_id, * from deliveries_v03 ; 
-- select * from deliveries_v04; 
 
-- Compare the total count of rows and total count of distinct ball_id in deliveries_v04.
select * from deliveries_v04 limit 20; 
select count(distinct ball_id) from deliveries_v04; select count(*) from deliveries_v04; 


-- Create table deliveries_v05 with all columns of deliveries_v04 and an additional column for row number partition over ball_id. (HINT :  row_number() over (partition by ball_id) as r_num) drop table deliveries_v05.
create table deliveries_v05 as select *, row_number() over (partition by ball_id) as r_num from deliveries_v04; 
select * from deliveries_v05; 
 
-- Use the r_num created in deliveries_v05 to identify instances where ball_id is repeating.(HINT : select * from deliveries_v05 WHERE r_num=2;)
select * from deliveries_v05 WHERE r_num=2; 
 
-- Use subqueries to fetch data of all the ball_id which are repeating.(HINT: SELECT * FROM deliveries_v05 WHERE ball_id in (select BALL_ID from deliveries_v05 WHERE r_num=2); 
SELECT * FROM deliveries_v05 WHERE ball_id in (select BALL_ID from deliveries_v05 WHERE r_num=2);

	
