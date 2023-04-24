-- cleaned data --> dtype conversion
WITH enrollments AS
(SELECT *
FROM `INTO_Live_Brief.Fall_Enrollments_2020`
UNION ALL
SELECT *
FROM  `INTO_Live_Brief.Fall_Enrollments_2019`
UNION ALL
SELECT *
FROM `INTO_Live_Brief.Fall_Enrollments_2018`
UNION ALL
SELECT *
FROM `INTO_Live_Brief.Fall_Enrollments_2017`
UNION ALL
SELECT *
FROM `INTO_Live_Brief.Fall_Enrollments_2016`
UNION ALL
SELECT *
FROM `INTO_Live_Brief.Fall_Enrollments_2015`
UNION ALL
SELECT *
FROM `INTO_Live_Brief.Fall_Enrollments_2014`
),

-- STRINGS TO INTS
b as (SELECT Unit_Id,
Institution_name,STABBR,
CASE
    WHEN Total = '-' THEN 0
     ELSE CAST(REPLACE(Total, ',', '') AS INT)
    END AS Total_int,
CASE
    WHEN In_state___Number = '-' THEN 0
     ELSE CAST(REPLACE(In_state___Number, ',', '') AS INT)
    END AS In_state___Number_int,
CASE
    WHEN Out_of_state___Number = '-' THEN 0
     ELSE CAST(REPLACE(Out_of_state___Number, ',', '') AS INT)
    END AS Out_of_state___Number_int,
CASE
    WHEN Foreign_countries___Number = '-' THEN 0
     ELSE CAST(REPLACE(Foreign_countries___Number, ',', '') AS INT)
    END AS Foreign_countries___Number_int,
CASE
    WHEN Unknown___Number = '-' THEN 0
     ELSE CAST(REPLACE(Unknown___Number, ',', '') AS INT)
    END AS Unknown___Number_int,
city,OBEREG,WEBADDR,ADMINURL,FAIDURL,NPRICURL,ICLEVEL,CAST(UGOFFER as int64) UGOFFER,HLOFFER,HBCU,
MEDICAL,LOCALE,OPENPUBL,CYACTIVE,C18UGPRF,
C18SZSET,C18BASIC,CCBASIC,COUNTYNM,year
FROM enrollments e
LEFT JOIN `INTO_Live_Brief.Data_Dictionary_2020` d
ON e.Unit_id = d.UNITID

-- filter by institutions offering UG (UGOFFER), are active (CYACTIVE), & aren't specialised/niche (C18BASIC)
where UGOFFER = 1 and cyactive = 1 and (CCBASIC not between 24 and 33) and (CCBASIC not between 10 and 13)

-- filter by intitutions offering 4-year-long UG programmes
and (C18UGPRF between 5 and 15) 

-- filter by institution size --> > 1 = 
and instsize > 1
ORDER BY year DESC, Foreign_countries___Number_int DESC),

-- % of internationals per intitution AND State FINAL with costs --> top 20

costs_join as (SELECT UNITID,SAFE_CAST(COSTT4_A AS INT64) as COSTT4_A,
SAFE_CAST(COSTT4_P AS INT64) as COSTT4_P,
institution_name, STABBR, sum(Foreign_countries___Number_int) foreign, 
sum(total_int) total, year 
FROM `prism-2023-c1.prism_test.FK_college_scorecard_costs_2020` costs
left join b 
on costs.UNITID = b.unit_id
group by UNITID,COSTT4_A,COSTT4_P, institution_name, STABBR,year
),

SUMS as (select institution_name, STABBR, sum(foreign) foreign, sum(total) total, year, COSTT4_A,COSTT4_P
from costs_join group by institution_name,STABBR,year,COSTT4_A,COSTT4_P),

DIVIDES as (select institution_name, STABBR, sum(foreign) foreign_total, sum(total) total,
round(safe_divide(sum(foreign), sum(total)),3) foreign_perc, year,COSTT4_A,COSTT4_P
from SUMS
group by institution_name,STABBR,year,COSTT4_A,COSTT4_P),

medians AS (select distinct STABBR,median  
from 
    (select *, PERCENTILE_CONT(t, 0.5 ignore nulls) over (partition by STABBR) as median 
    from 
        (SELECT COSTT4_A t,STABBR from DIVIDES
        union all
       
       select COSTT4_P,STABBR FROM DIVIDES
    ))
    ),

table as
    (
    select *, 
    round(safe_divide((foreign_perc - lag(foreign_perc,1)
    over (partition by institution_name order by year)),
    lag(foreign_perc,1) over (partition by institution_name order by year)),2) as lag,

    from 
        (
        select institution_name,STABBR,sum(foreign_total) foreign_total, sum(total) sum_total,
        sum(IFNULL(foreign_perc,0)) as foreign_perc, year,COSTT4_A,COSTT4_P
        from DIVIDES
        group by institution_name,STABBR,year,COSTT4_A,COSTT4_P
        ) 
    ),

FINAL as (
    select institution_name,medians.STABBR,foreign_total,sum_total,foreign_perc,year,COSTT4_A,COSTT4_P,lag,
    median, CASE WHEN COSTT4_A > median OR COSTT4_P > median THEN 1 ELSE 0 end as midpoint
    from medians
    join
    (SELECT institution_name, STABBR, foreign_total, sum_total, foreign_perc, year, ifnull(COSTT4_A,0) COSTT4_A,
    ifnull(COSTT4_P,0) COSTT4_P,IFNULL(lag,0) lag
    FROM table
    ) final2
    on medians.STABBR = final2.STABBR
), 

top_10_ca as (select distinct institution_name from (
-- select distinct institution_name from (
select institution_name, STABBR,foreign_total,sum_total,foreign_perc,year,ifnull(COSTT4_A,0) COSTT4_A,
ifnull(COSTT4_P,0) COSTT4_P,lag,median,
CASE WHEN COSTT4_A > median OR COSTT4_P > median THEN 1 ELSE 0 end as midpoint

from final
where institution_name in (
    select institution_name 
    from (

        select institution_name,countif(lag2 < 0) dec_total, countif(lag < 0) dec_perc 
        from (
            select institution_name, foreign_total, foreign_perc, lag, year, foreign_total - 
            lag(foreign_total) over (partition by institution_name
            order by year) lag2
            from final
            where institution_name in (
                select institution_name 
                from (
                    select distinct institution_name, ifnull(COSTT4_A,0) COSTT4_A, ifnull(COSTT4_P,0) COSTT4_P
                    from final 
                    where ((stabbr = 'TX' and foreign_perc > 0.04) OR (stabbr = 'CA' and foreign_perc > 0.16))
                    )
                )
            )
        group by 1 having countif(lag2 < 0) > 2 or countif(lag < 0) > 2 or institution_name like '%Westcliff Uni%'
        )) and (stabbr = 'TX' or stabbr = 'CA') 
        and stabbr = 'CA'
order by foreign_total desc limit 100) limit 10),


top_10_tx as (select distinct institution_name from (
-- select distinct institution_name from (
select institution_name, STABBR,foreign_total,sum_total,foreign_perc,year,ifnull(COSTT4_A,0) COSTT4_A,
ifnull(COSTT4_P,0) COSTT4_P,lag,median,
CASE WHEN COSTT4_A > median OR COSTT4_P > median THEN 1 ELSE 0 end as midpoint
from final
where institution_name in (
    select institution_name 
    from (
        select institution_name,countif(lag2 < 0) dec_total, countif(lag < 0) dec_perc 
        from (
            select institution_name, foreign_total, foreign_perc, lag, year, foreign_total - 
            lag(foreign_total) over (partition by institution_name
            order by year) lag2
            from final
            where institution_name in (
                select institution_name 
                from (
                    select distinct institution_name, ifnull(COSTT4_A,0) COSTT4_A, ifnull(COSTT4_P,0) COSTT4_P
                    from final 
                    where ((stabbr = 'TX' and foreign_perc > 0.04) OR (stabbr = 'CA' and foreign_perc > 0.16))
                    )
                )
            )
        group by 1 having countif(lag2 < 0) > 2 or countif(lag < 0) > 2 -- or institution_name like '%Westcliff Uni%'
        )) and (stabbr = 'TX' or stabbr = 'CA') -- ORDER BY stabbr,institution_name,year 
        and stabbr = 'TX'
order by foreign_total desc limit 100) limit 10)

select * from final where institution_name in (select institution_name from (select * from top_10_ca))
union all
select * from final where institution_name in (select institution_name from (select * from top_10_tx))
