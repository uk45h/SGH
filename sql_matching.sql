use role accountadmin;
use database sgh;
use schema public;
use warehouse compute_wh;


-- set up dev environment


create warehouse if not exists transforming warehouse_size = 'small' auto_suspend = 120 initially_suspended=true;
alter warehouse TRANSFORMING set warehouse_size = 'SMALL';

use warehouse transforming;


-- define the standardize function
create or replace function standardize(a String) 
returns string 
strict immutable 
COMMENT = 'Removes non-alphanumeric characters and casts the result to UPPER case'
as $$ select REGEXP_REPLACE(UPPER(a),'[^A-Z0-9ĄĆĘŁŃÓŚŹŻ ]', '') $$;


CREATE OR REPLACE TABLE SGH.PUBLIC.BAZA_KLIENTOW_ALL AS
    SELECT 
        PESEL, 
        IMIE, 
        NAZWISKO, 
        PLEC, 
        DATA_URODZENIA, 
        MIASTO_ZAMELDOWANIA, 
        ULICA_ZAMIESZKANIA, 
        NUMER_DOMU, 
        GRUPA, 
        ID_TARYFY, 
        TEL_USED, 
        TEL_SOLD, 
        CZAS_W_MIESIACU, 
        CZAS_DZISIAJ, 
        STATUS, 
        LICZBA_PUNKTOW,
        '' AS KOD_POCZTOWY,
        '' AS E_MAIL
    FROM SGH.PUBLIC.BAZA_KLIENTOW
    
    UNION ALL
    
    SELECT
        PESEL, 
        IMIE, 
        NAZWISKO, 
        PLEC, 
        DATA_URODZENIA, 
        MIEJSCE_ZAMIESZKANIA AS MIASTO_ZAMELDOWANIA, 
        ULICA_ZAMIESZKANIA, 
        NUMER_DOMU, 
        ' ' AS GRUPA, 
        NULL::NUMBER AS ID_TARYFY, 
        NULL::NUMBER AS TEL_USED, 
        NULL::NUMBER AS TEL_SOLD, 
        NULL::NUMBER AS CZAS_W_MIESIACU, 
        NULL::NUMBER AS CZAS_DZISIAJ, 
        NULL::BOOLEAN AS STATUS, 
        NULL::NUMBER AS LICZBA_PUNKTOW,
        KOD_POCZTOWY, 
        E_MAIL
    FROM SGH.PUBLIC.BAZA_KLIENTOW_WWW;

SELECT * FROM SGH.PUBLIC.BAZA_KLIENTOW_ALL LIMIT 10;


-- create a samples data set derived from the TPCDS sample datasets - standardize these fields while creating the table
create or replace transient table SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD as
    select 
            PESEL, 
            standardize(IMIE) AS IMIE, 
            standardize(NAZWISKO) AS NAZWISKO, 
            standardize(PLEC) AS PLEC, 
            DATA_URODZENIA, 
            standardize(MIASTO_ZAMELDOWANIA) AS MIASTO_ZAMELDOWANIA, 
            trim(standardize(ULICA_ZAMIESZKANIA)) AS ULICA_ZAMIESZKANIA, 
            standardize(NUMER_DOMU) AS NUMER_DOMU, 
            standardize(GRUPA) AS GRUPA, 
            ID_TARYFY, 
            TEL_USED, 
            TEL_SOLD, 
            CZAS_W_MIESIACU, 
            CZAS_DZISIAJ, 
            STATUS, 
            LICZBA_PUNKTOW,
            standardize(KOD_POCZTOWY) AS KOD_POCZTOWY,
            standardize(E_MAIL) AS standardize
    from SGH.PUBLIC.BAZA_KLIENTOW_ALL;

CREATE OR REPLACE SEQUENCE seq1;

create or replace transient table SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD_ID as
    select 
        seq1.NEXTVAL AS CUST_ID,
        *
        from SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD;

 SELECT * FROM SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD_ID LIMIT 10;

--define out fuzzy_scoring UDF
create or replace function fuzzy_score(a String, b String)
returns number
strict
immutable
COMMENT = 'Takes two strings and returns a similarity score between 1 and 0'
as 'select 1.0-(editdistance(a, b)/greatest(length(a),length(b)))'; 


-- use this query to check the number of pairs that will be generated based on your blocking key
select
    sum(pairs) pairs from (
select
    count(*) as recs,
    (count(*)*(count(*)))::number(12,0) pairs
from SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD_ID); 

select
    sum(pairs) pairs from (
select
    imie,
    count(*) as recs,
    (count(*)*(count(*)))::number(12,0) pairs
from SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD
group by 1);  

select imie, count(*) from SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD_ID group by 1 order by 2 desc;

--Let's assume that imie is correct to minimalize cartesian joins
select count(*)
from SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD_ID t1
inner join SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD_ID t2 where t1.imie='HILARIA' and t2.imie='HILARIA';


select count(*)
from SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD_ID t1
inner join SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD_ID t2 where t1.imie='HILARIA' and t2.imie='HILARIA' and t1.CUST_ID<t2.CUST_ID;


/*
	Generate pairs for downstream scoring evaluation
	Note this uses ZIP as the blocking key
	Consider sorting the source table by the zip and cust_key for better performance
	create or replace table samples as (select * from samples order by zip, cust_key);
*/
-- ALTER WAREHOUSE TRANSFORMING SET WAREHOUSE_SIZE = large;

create or replace transient table candidate_pairs as (
    select
    t1.cust_id as cust_id1
    , t2.cust_id as cust_id2
    , t1.imie as fname1 
    , t2.imie as fname2
    , t1.nazwisko as lname1 
    , t2.nazwisko as lname2
    , t1.MIASTO_ZAMELDOWANIA as address1 
    , t2.MIASTO_ZAMELDOWANIA as address2
from SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD_ID t1
inner join SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD_ID t2 on t1.imie = t2.imie and t1.cust_id < t2.cust_id)
;

-- ALTER WAREHOUSE TRANSFORMING SET WAREHOUSE_SIZE = xsmall;


SELECT * FROM candidate_pairs LIMIT 10;
--flex the warehouse up, run the scoring then flex back down again
-- alter warehouse TRANSFORMING set warehouse_size = 'XLARGE';

create or replace transient table scores as( 
select
    cust_id1 || '-' || cust_id2 pair_key
    , cust_id1 
    , cust_id2 
    , fuzzy_score(fname1, fname2) fname_fuzzy
    , fuzzy_score(soundex(fname1), soundex(fname2)) fname_soundex
    , fuzzy_score(lname1, lname2) lname_fuzzy 
    , fuzzy_score(soundex(lname1), soundex(lname2)) lname_soundex
    , fuzzy_score(address1, address2) address_fuzzy
from candidate_pairs);

select * from scores limit 100;

-- alter warehouse TRANSFORMING set warehouse_size = 'XSMALL'; 
select fuzzy_score('526 12TH CIR','390 13TH DR');

-- Look at some records that are potential duplicates - play with the thresholds and other score values to see compared records in over/under comparison
with candidates as (
    select 
        pair_key,
        cust_id1,
        cust_id2
        from scores
        where fname_soundex >= .40 and fname_fuzzy >= .40 and lname_fuzzy >= .40 and address_fuzzy >= 0.40
)
select
    pair_key,
    b.*
from candidates c
inner join SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD_ID b on cust_id = cust_id1
union
select
    pair_key,
    b.*
from candidates c
inner join SGH.PUBLIC.BAZA_KLIENTOW_ALL_STD_ID b on b.cust_id = c.cust_id2
order by pair_key, cust_id
limit 100;
