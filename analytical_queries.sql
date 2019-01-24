-- What is the average like rate (likes/(likes+skips))?
select (a.count_likes *1.00/ b.count_likes_plus_skips) *100 as average_like_rate from (
		select count(distinct (connection_nbr||least_key)) as count_likes
			from public.event_staging
       		where rating_type in (1,2)) a, 
       (
       select count(distinct (connection_nbr||least_key)) as count_likes_plus_skips
          	from public.event_staging
       		where rating_type in (0,1,2)) b

-- What percentage of likes go unresponded to?
select (a.count_a * 1.00 / b.count_b) * 100 as percentage_likes_go_unresponded from 
       (select count(distinct (lconn)) as count_a
  			from (select distinct (connection_nbr||least_key) as lconn
        	from public.event_staging
        	where rating_type in (1,2) ) l,
        (select distinct (connection_nbr||least_key) as rconn
          	from public.event_staging
           	where rating_type in (5)  ) r
  		 	where rconn = lconn) a,
     	(select count(distinct (connection_nbr||least_key)) as count_b
      		from public.event_staging
     		where rating_type in (1,2)) b

-- What % of reports happen after two users connect as opposed to before?
select count(1)
  from public.event_staging n
where rating_type = 4
   and not exists (select *
                     from public.event_staging r
                          where rating_type in (1,2)
                                    and r.connection_nbr = n.connection_nbr
                                    and r.least_key = n.least_key)
 
select count(1)
  from public.event_staging n
where rating_type = 4
   and exists (select *
                     from public.event_staging r
                          where rating_type in (1,2)
                                    and r.connection_nbr = n.connection_nbr
                                    and r.least_key = n.least_key)

-- What is the average number of skips before a like?
select count(1), count(distinct s.connection_nbr||s.least_key)
  from public.event_staging s,
       public.event_staging l
where l.connection_nbr = s.connection_nbr
   and l.least_key = s.least_key
   and s.rating_type = 0
   and l.rating_type in (1,2)
   and s.event_timestamp < l.event_timestamp


-- In discover, how often do people see duplicates subjects within a day (assume every subject is either skipped or liked)?
select count(distinct(to_char(event_timestamp,'YYYYMMDD')||connection_nbr||least_key))
  	from public.event_staging
	where rating_type in (0,1,2)
 
select to_char(event_timestamp,'YYYYMMDD')||connection_nbr||least_key, subject_id, count(1)
	from public.event_staging
	where rating_type in (0,1,2)
	group by 1,2
having count(1) > 1


-- How often do people change their mind about the people they like?
-- Divide the below 2 queries to get a Percentage..
select count(distinct (lconn))
  from (select distinct (connection_nbr||least_key) as lconn
          from public.event_staging
              where rating_type in (1,2) ) l,
        (select distinct (connection_nbr||least_key) as rconn
          from public.event_staging
              where rating_type in (3,4)  ) r
   where rconn = lconn
   
select count(distinct (connection_nbr||least_key))
          from public.event_staging
       where rating_type in (1,2)

