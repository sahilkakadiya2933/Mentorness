
use game_analysis;

-- Problem Statement - Game Analysis dataset
-- 1) Players play a game divided into 3-levels (L0,L1 and L2)
-- 2) Each level has 3 difficulty levels (Low,Medium,High)
-- 3) At each level,players have to kill the opponents using guns/physical fight
-- 4) Each level has multiple stages at each difficulty level.
-- 5) A player can only play L1 using its system generated L1_code.
-- 6) Only players who have played Level1 can possibly play Level2 
--    using its system generated L2_code.
-- 7) By default a player can play L0.
-- 8) Each player can login to the game using a Dev_ID.
-- 9) Players can earn extra lives at each stage in a level.

alter table pd modify L1_Status varchar(30);
alter table pd modify L2_Status varchar(30);
alter table pd modify P_ID int primary key;
alter table pd drop myunknowncolumn;

alter table ld drop myunknowncolumn;
alter table ld change timestamp start_datetime datetime;
alter table ld modify Dev_Id varchar(10);
alter table ld modify Difficulty varchar(15);
alter table ld add primary key(P_ID,Dev_id,start_datetime);

-- pd (P_ID,PName,L1_status,L2_Status,L1_code,L2_Code)
-- ld (P_ID,Dev_ID,start_time,stages_crossed,level,difficulty,kill_count,
-- headshots_count,score,lives_earned)


-- Q1) Extract P_ID,Dev_ID,PName and Difficulty_level of all players 
-- at level 0

select P_ID, Dev_ID, PName, difficulty from ld
left join pd 
on pd.P_ID = ld.P_ID
where ld.level = 0;

-- Q2) Find Level1_code wise Avg_Kill_Count where lives_earned is 2 and atleast
--    3 stages are crossed

select pd.L1_Status,avg(Kill_Count) from ld
left join pd 
on pd.P_ID = ld.P_ID
where ld.Lives_Earned = 2 and ld.Stages_Crossed>=3
group by pd.L1_Status;

-- Q3) Find the total number of stages crossed at each diffuculty level
-- where for Level2 with players use zm_series devices. Arrange the result
-- in decsreasing order of total number of stages crossed.

select sum(Stages_crossed) as `total`, Difficulty from ld
left join pd 
on pd.P_ID = ld.P_ID
where ld.Dev_ID like "zm%" and ld.Level = 2
group by ld.Difficulty
order by `total` desc;

-- Q4) Extract P_ID and the total number of unique dates for those players 
-- who have played games on multiple days.

SELECT count(DISTINCT(DATE(TimeStamp))) as unique_dates, P_ID FROM ld
group by P_ID
having count(distinct(TimeStamp))>1;

-- Q5) Find P_ID and level wise sum of kill_counts where kill_count
-- is greater than avg kill count for the Medium difficulty.

SELECT P_ID, Level, sum(Kill_Count) FROM  ld
where Kill_Count> 
	(select avg(Kill_Count) from ld where Difficulty="Medium")
group by P_ID, Level;

-- Q6)  Find Level and its corresponding Level code wise sum of lives earned 
-- excluding level 0. Arrange in asecending order of level.

SELECT ld.Level, pd.L1_Code, pd.L2_Code, sum(ld.Lives_Earned) FROM  ld
join pd on ld.P_ID = pd.P_ID
where ld.Level!= 0 
group by ld.Level, pd.L1_Code, pd.L2_Code
order by ld.Level;


-- Q7) Find Top 3 score based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well. 

with new_table as     (
  select
    Dev_ID,
    Difficulty, 
    Score, 
    row_number() over (partition by Dev_ID order by Score desc) as Ranked
  from
    ld
)
select 
  Dev_ID, Score, Difficulty Ranked 
from
  new_table
where Ranked <= 3;

-- Q8) Find first_login datetime for each device id

select Dev_ID, min(TimeStamp)
from ld
group by Dev_ID;

-- Q9) Find Top 5 score based on each difficulty level and Rank them in 
-- increasing order using Rank. Display dev_id as well.

with new_table as     (
  select
    Dev_ID,
    Score,
    Difficulty, 
    rank() over (partition by Difficulty order by Score desc) as ranked
  from
  ld   
)
select 
  Dev_ID, Difficulty, Score, ranked 
from
  new_table
where Ranked < 6;

-- Q10) Find the device ID that is first logged in(based on start_datetime) 
-- for each player(P_ID). Output should contain player id, device id and 
-- first login datetime.

select

P_ID, Dev_ID, min(TimeStamp)
from
ld
group by Dev_ID, P_ID;

-- Q11) For each player and date, how many kill_count played so far by the player. That is, the total number of games played -- by the player until that date.

-- a) window function

select distinct P_ID, cast(TimeStamp as Date) as Dated, sum(Kill_Count) over (partition by P_ID, cast (TimeStamp as Date) order by Cast(TimeStamp as Date))
from
ld
order by
P_ID, Dated;


-- b) without window function

select
P_ID, cast(TimeStamp as Date) as Dated, sum(Kill_Count)
from
ld
group by
P_ID,
Cast(TimeStamp as Date)
order by
P_ID, Dated;

-- Q12) Find the cumulative sum of an stages crossed over a start_datetime 
-- for each player id but exclude the most recent start_datetime

with task as (select
P_ID, Stages_crossed, TimeStamp, row_number()
over(partition by P_ID order by TimeStamp desc) as rn
from
ld
)
select
P_ID, sum(Stages_crossed), TimeStamp 
from
task
where rn>1
group by P_ID, TimeStamp;


-- Q13) Extract top 3 highest sum of score for each device id and the corresponding player_id
select
P_ID, Dev_ID, sum(Score) as score, row_number()
over(partition by Dev_ID order by sum(Score) desc) as
Ranked 
from
ld
group by Dev_ID, P_ID;

select Dev_ID, P_ID, score from task where Ranked<4;


-- Q14) Find players who scored more than 50% of the avg score scored by sum of 
-- scores for each player_id

select
P_ID, sum(Score) from ld
group by P_ID 
having sum(Score)>0.5 *
  (
    select 
      avg(Score) 
    from
    ld
  );
  
-- Q15) Create a stored procedure to find top n headshots_count based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well.

DELIMITER //
CREATE procedure TopN(IN n INT)
BEGIN
	select Dev_Id, Headshots_Count, Difficulty
    from (
		select
			Dev_ID,
            Headshots_Count,
            Difficulty,
            row_number() over (partition by Dev_ID order by Headshots_Count) as ranked
            from
            ld
	) as task
    where ranked <= n;
    End//
    DELIMITER ;
    call TopN(6)
    

 