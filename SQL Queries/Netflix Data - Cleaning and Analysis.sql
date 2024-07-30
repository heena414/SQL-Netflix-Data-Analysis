
/*SQL PROJECT - NETFLIX DATA: CLEANING AND ANALYSIS*/



Create database NetflixData;

use NetflixData;

select* from netflix;




--DATA CLEANING--



--1. Check Duplicate Values and Remove 

delete from netflix
where show_id in(
select show_id from(
select show_id, row_number() over(partition by show_id order by show_id ) as row_num 
from netflix
) temp_table 
where row_num >1 );                                                       -- O rows affected whch means there are only unique values of show_id

--2. Handling Missing Values

Select * from netflix  where director is null; 

Select count(*) as type_null from netflix where type is null;              --0
Select count(*) as title_null from netflix where title is null;            --24 null values of title 
Select count(*) as director_null from netflix where director is null;      --2634 null values of directors 
Select count(*) as cast_null from netflix where cast is null;              --825 null values of cast
Select count(*) as country_null from netflix where country is null;        --831 null values of country
Select count(*) as date_null from netflix where date_added is null;        --10 null values of date_added
Select count(*) as ryear_null from netflix where release_year is null;     --0
Select count(*) as taing_null from netflix where rating is null;           --4 null values of rating
Select count(*) as duration_null from netflix where duration is null;      --3 null values of rating
Select count(*) as listed_null from netflix where listed_in is null;       --0


--TODO: Since date_added, ratiing, duration have less number of rows, deleting them will not affect the analysis and visualisation

select* from netflix;

select * from netflix 
where
date_added is null
or duration is null
or rating is null;

delete from netflix
where
date_added is null
or duration is null
or rating is null;     --17 rows affected


--TODO: Finding a corelation between director and cast
--(Directors likely to work with a particular cast)

select director, cast 
from netflix 
where cast is null or director is null;      --3094 rows

--With this, we can now fill NULL rows in directors using their record with movie_cast
with CTE as 
(
select title, concat(director, ' ---- ' , cast) as director_cast
from netflix
)
select director_cast, count(*) as count
from CTE
group by director_cast 
having count(*) >1 
order by count(*) desc;

--finding the relation between cast and director and filling out the missing values 
select* from netflix;

update netflix 
set director = 'Alastair Fothergill'
where cast = 'David Attenborough'
and director is null;                                 

update netflix 
set cast = 'Vatsal Dubey, Julie Tejwani, Rupa Bhimani, Jigna Bhardwaj, Rajesh Kava, Mousam, Swapnil'
where director = 'Rajiv Chilaka'
and cast is null;

update netflix
set director = 'S.S. Rajamouli'
where cast = 'Prabhas'
and director is null;

update netflix 
set director = 'Rathindran R Prasad'
where cast = 'Aishwarya Rajesh, Vidhu, Surya Ganapathy, Madhuri, Pavel Navageethan, Avantika Vandanapu'
and director is null;

update netflix 
set director = 'Matías Gueilburt'
where cast = 'Paco Ignacio Taibo II'
and director is null;

update netflix 
set director = 'Oliver Twinch'
where cast = 'Craig Sechler'
and director is null;

--filling remaining Null values of Directors with Unknown
update netflix
set director = 'Unknown'
where director is null; --2603 unknown directors


--TODO: Dealing with null values of Country
-- Since the country column is related to director and movie, we are going to fill the country column with the director column

select distinct director, country 
from netflix
where country is null;        --341 

select distinct director, country 
from netflix
where country is not null; --4839 rows

--filling out the country using the director column

with KnownCountries as (
select director, max(country) as country
from netflix
where country is not null
group by director
)
update n
set n.country = kc.country
from netflix n
join KnownCountries kc on n.director = kc.director
where n.country is null;  --554(overall) rows affected

-- dealing with remaining null values of country

update netflix
set country = 'Not Known'
where country IS NULL; --275 rows affected


update netflix 
set country = 'Not known'
where country = ' ';

select*from netflix;


--TODO: treating title null values

select title, director, type, release_year 
from netflix 
where title is null;     --24 rows

update netflix
set title = 'Not Given'
where title is null;


--TODO: treating title null values

select cast 
from netflix 
where cast is null;   -- 823 rows of null cast value

update netflix 
set cast = 'Missing'
where cast is null;


--check to confirm the number of rows are the same (that means no null value)

Select count(*) as type_null from netflix where type is not null;             
Select count(*) as title_null from netflix where title is not null;            
Select count(*) as director_null from netflix where director is not null;                  
Select count(*) as country_null from netflix where country is not null;        
Select count(*) as date_null from netflix where date_added is not null;     
Select count(*) as ryear_null from netflix where release_year is not null;
Select count(*) as taing_null from netflix where rating is not null;          
Select count(*) as duration_null from netflix where duration is not null;      
Select count(*) as listed_null from netflix where listed_in is not null;    --no null value is present, as all columns have same no of rows



--3. Drop nuneeded columns

alter table netflix 
drop column description;

select*from netflix;


--4. STANDARISED THE DATA


--TODO: there are rows that have multiple countries ---> split the country column and retain the first country by the left (charindex and left function)

select* from netflix
where charindex(',', country) > 0; --1745 rows with multiple country name

update netflix
set country = left(country, charindex(',', country + ',') - 1)
where charindex(',', country) > 0;    --1745 rows affected

select* from netflix;

--TODO: Correct inconsistent data

update netflix
set country = TRIM(trailing '.' from country);

update netflix
set country = trim(leading '.' from country);

--TODO: changing the format of date_added

UPDATE netflix
SET date_added = CONVERT(DATE, date_added, 101); --101 for - yyyy.mm.dd




--DATA ANALYSIS--



--1. Total Count/ Entries in data

select count(*) as total_count 
from netflix;

--2. No of Movies and Tv shows

select*from netflix;

select type, count(*) as count 
from netflix 
group by type;

--3. Proportion of movies vs tv shows

select type, count(*) as count,
cast(count(*) * 100.0 / (select count(*) from netflix) as decimal(5,2) ) as percentage
from netflix
group by type;

--4. Average Duration of Movie (in min)

select avg(cast(substring(duration, 1, len(duration) - 4) as integer)) as average_duration 
from netflix 
where type = 'Movie';

--5. Distribution of Ratings

select rating, count(*) as rating_count 
from netflix 
group by rating;

--6. Top 10 Genre

select top 10 listed_in, count(*) as genre_count 
from netflix
group by listed_in 
order by genre_count desc; 

--7. Year wise release count 
--Content addition to Netflix over the Years

select release_year, count(*) as year_count 
from netflix 
group by release_year 
order by release_year;

--8. Top 10 Year having max release count

select top 10 release_year, count(*) as topyear_count 
from netflix 
group by release_year 
order by release_year desc;

--9. Country wise content count

select country, count(*) as country_count 
from netflix 
group by country 
order by country;

--10. Top countries having max release count

select top 10 country, count(*) as country_count
from netflix
group by country 
order by country_count desc;

--11. Popularity of Different Genrese in various countries

select country, listed_in, count(*) as genre_count 
from netflix
group by country, listed_in
order by country, genre_count desc;

--12. Top 10 countries with most popular genre

select top 10 country, listed_in, count(*) as genre_count 
from netflix
group by country, listed_in
order by genre_count desc; --(competition b/w India and US broo)

--13. Seasonal pattern in content release 

select month(date_added) as release_month,
count(*) as num_titles_added
from netflix
group by month(date_added)
order by release_month;

--14. Max release year

select max(release_year) as max_year 
from netflix;

--15. Min release year

select min(release_year) as min_year 
from netflix;

--16. Year when content added to netflix

select min(date_added) as min_year 
from netflix;

select max(date_added) as max_year 
from netflix;

--17.  Trends in length of content over time

select release_year,
avg(case 
when type = 'Movie' and isnumeric(substring(duration, 1, len(duration) - 4)) =1
then cast(substring(duration, 1, len(duration) - 4) as float) 
else null 
end)  as avg_movie_duration,
avg(case
when type = 'TV Show' and isnumeric(substring(duration, 1, len(duration) - 8)) =1
then cast(substring(duration, 1, len(duration) - 8) as float) 
else null
end) as avg_tv_show_seasons
from netflix
group by release_year
order by release_year;

--18. Average duration of movies and seasons for Tv shows

select type,
avg(case 
when type = 'Movie' and isnumeric(substring(duration, 1, len(duration) - 4)) =1
then cast(substring(duration, 1, len(duration) - 4) as float) 
else null  
end) as avg_movie_duration,
avg(case
when type = 'TV Show' and isnumeric(substring(duration, 1, len(duration) - 8)) =1
then cast(substring(duration, 1, len(duration) - 8) as float) 
else null
end) as avg_tv_show_seasons
from netflix
group by type;




--CREATING A NEW TABLE TO STORE INDIVIDUAL CAST MEMBERS 



drop table individual_cast;

select max(len(trim(cast))) as max_length
from netflix
cross apply string_split(cast, ',');

create table individual_cast
(
show_id varchar(10),
cast_member varchar(800)
);

select* from individual_cast;

insert into individual_cast
(show_id , cast_member)
select
show_id,
trim(cast) as cast_member
from netflix
cross apply string_split(cast, ',')    --64853 rows affected
;

select top 10* from individual_cast;



--19. Total number of unique cast members

select count(distinct cast_member) as unique_member 
from individual_cast;     --7679

--20. Total Number of Shows/Movies per Cast Member

select cast_member, count(*) as appearance_count
from individual_cast
group by cast_member
order by cast_member desc;

--21. Top 10 most frequent Cast Members

select * from netflix;

select top 10 cast_member, count(*) as count
from individual_cast
group by cast_member
order by cast_member desc;

--22. Total Number of shows

select count(distinct show_id) as total_shows
from individual_cast;

--23. Average cast size per show

select avg(CastSize) as average_cast_size
from(
select show_id, 
count(cast_member) as CastSize
from individual_cast
group by show_id
) as ShowCastSizes;

--24. Cast Member Participation Rate

select cast_member, 
concat(round(count(show_id) * 1.0 / (select count(distinct show_id) from individual_cast)*100, 2), '%') as participation_rate
from individual_cast
group by cast_member;

--25. Distribution of Cast Members by show type

select n.type, count(distinct ic.cast_member) as unique_cast_members
from netflix as n
join individual_cast as ic 
on n.show_id = ic.show_id
group by n.type;

--26. Cast Members by Country of Show

select n.country, 
count(distinct ic.cast_member) as unique_cast_members
from netflix as n
join individual_cast as ic 
on n.show_id = ic.show_id
group by n.country;

--27. Count of Cast Members appearing in multiple genres

select 
ic.cast_member, 
count(distinct n.listed_in) as gentre_count
from 
individual_cast as ic
join 
netflix as n 
on ic.show_id = n.show_id
group by cast_member
having count(distinct listed_in) > 1
order by count(distinct n.listed_in) desc;

--28. Top 5 cast members by release year

select top 5 
n.release_year, ic.cast_member, count(*) as count_
from individual_cast as ic
join netflix as n 
on ic.show_id = n.show_id
group by n.release_year, ic.cast_member
order by n.release_year, count_ desc;

--29. Most Popular Director - Cast pairs

select n.director, ic.cast_member,
count(*) as collaborations
from individual_cast as ic
join netflix as n 
on ic.show_id = n.show_id
group by n.director, ic.cast_member
order by collaborations desc;

--30. Top 5 most popular Director - Cast pairs

select top 5 n.director, ic.cast_member,
count(*) as collaborations
from individual_cast as ic
join netflix as n 
on ic.show_id = n.show_id
where director != 'Unknown' and cast_member != 'Missing'
group by n.director, ic.cast_member
order by collaborations desc;

--31. Number of Unique Shows/Movies per cast Member

select cast_member, count(distinct show_id) as unique_shows
from individual_cast
group by cast_member
order by unique_shows desc;

--32. Cast Member apperaing in both movies and tv shows

select ic.cast_member
from individual_cast as ic
join netflix as n 
on ic.show_id = n.show_id
group by ic.cast_member
having count(distinct n.type)>1; 

--33. Top 5 shows with the most Cast Members

select show_id, count(cast_member) as cast_count
from individual_cast
group by show_id
order by cast_count desc
offset 0 rows fetch next 5 rows only;

--34. Cast Diversity Index (Unique Cast Members per show)

select show_id, 
round(count(distinct cast_member) * 1.0 / count(cast_member),2) as diversity_index
from individual_cast
group by show_id;

--35. Cast Members with most recent release

select ic.cast_member, max(n.release_year) as most_recent_year
from individual_cast as ic
join netflix as n
on ic.show_id = n.show_id
group by ic.cast_member
order by  most_recent_year desc;

--36. Cast Members withh the most diverse roles

select ic.cast_member,
count(DISTINCT n.listed_in) as distinct_roles
from individual_cast as ic
join netflix as n 
on ic.show_id = n.show_id
group by ic.cast_member
order by distinct_roles desc;

select ic.cast_member, 
count(DISTINCT n.listed_in) as distinct_roles
from individual_cast as ic
join netflix as n 
on ic.show_id = n.show_id
group by ic.cast_member
having count(distinct n.listed_in)>2
order by distinct_roles desc;

--37. Distribution of Content Release by year for top 10 cast members

with Top10Cast as(
select top 10 cast, count(*) as 'count'
from netflix
group by cast
order by 'count' desc
)
select n.cast, n.release_year, count(*) as release_count
from netflix as n
join Top10Cast as t 
on n.cast = t.cast
group by n.cast, n.release_year
order by n.cast, n.release_year;

--38. Average Duration of Movies/TV Shows per cast member

select cast, 
avg(
case when type = 'Movie' 
then CAST(substring(duration, 1, len(duration) - 4) as int) 
end) as avg_movie_duration,
avg(
case when type = 'TV Show' 
then cast(substring(duration, 1, len(duration) - 8) as int) 
end) as avg_tv_show_duration
from netflix
group by cast;

