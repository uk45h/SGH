use role accountadmin;
use sgh.public;

-- define the standardize function
create or replace function standardize(a String) 
returns string 
strict immutable 
COMMENT = 'Removes non-alphanumeric characters and casts the result to UPPER case'
as $$ select REGEXP_REPLACE(UPPER(a),'[^A-Z0-9ĄĆĘŁŃÓŚŹŻ ]', '') $$;


-- create a samples data set derived from the TPCDS sample datasets - standardize these fields while creating the table
create or replace transient table samples as(
select 
	standardize(a.c_customer_sk) as cust_key
	, standardize(a.c_current_addr_sk) as add_key
	, standardize(a.c_first_name) as fname
	, standardize(a.c_last_name) as lname
	, standardize(concat(b.ca_street_number, ' ', b.ca_street_name, ' ', b.ca_street_type)) as address
	, standardize(b.ca_city) as city
	, standardize(b.ca_state) as state
	, standardize(b.ca_zip) as zip
from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."CUSTOMER" a
inner join "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."CUSTOMER_ADDRESS" b
on a.c_current_addr_sk = b.ca_address_sk
where a.c_first_name is not null and
a.c_last_name is not null and
address is not null and
ca_state is not null and
ca_zip is not null)
limit 500000; --dial this down to limit the amount of computing that gets used later


-- Define a dictionary table
create or replace transient table dictionary (
token String not null,
term String not null,
context String not null,
constraint UK unique (token, context)
);

/*
	Lets look for some candidate names to add to the dictionary
	get back all the fnames that are less than 5 characters
*/
select distinct fname from samples where len(fname) < 5;
select * from samples limit 100;
--MAX is one of the returned values, let's see what other names in the SAMPLES align to MAX
select distinct fname from samples where fname like ('MAX%');

--add some name terms to the dictionary
insert into dictionary values ('MAXINE', 'MAX', 'NAME'), ('MAXIMO', 'MAX', 'NAME'), ('MAXIE', 'MAX', 'NAME'), ('MAXWELL', 'MAX', 'NAME');
insert into dictionary values ('JEFFREY', 'JEFF', 'NAME'), ('JEFFERY', 'JEFF', 'NAME'), ('JEFFRY', 'JEFF', 'NAME'), ('JEFFIE', 'JEFF', 'NAME'), ('JEFFERSON', 'JEFF', 'NAME');
insert into dictionary values ('JACKIE', 'JACK', 'NAME'), ('JACKSON', 'JACK', 'NAME'), ('JACKI', 'JACK', 'NAME'), ('JACKLYN', 'JACK', 'NAME'), ('JACKELYN', 'JACK', 'NAME');
insert into dictionary values ('LEONA', 'LEO', 'NAME'), ('LEONARDO', 'LEO', 'NAME'), ('LEONILA', 'LEO', 'NAME'), ('LEOLA', 'LEO', 'NAME'), ('LEON', 'LEO', 'NAME'), ('LEONARD', 'LEO', 'NAME'), ('LEONIDA', 'LEO', 'NAME');

SELECT * FROM dictionary;

--add some address terms to the dictionary
insert into dictionary values ('AVENUE', 'AVE', 'ADDRESS'), ('STREET', 'ST', 'ADDRESS'), ('BOULEVARD', 'BLVD', 'ADDRESS'), ('COURT', 'CT', 'ADDRESS'), ('LANE', 'LN', 'ADDRESS'), ('CIRCLE', 'CIR', 'ADDRESS'), ('PARKWAY', 'PKWY', 'ADDRESS'), ('WAY', 'WY', 'ADDRESS'), ('DRIVE', 'DR', 'ADDRESS');
insert into dictionary values ('FIRST', '1ST', 'ADDRESS'), ('SECOND', '2ND', 'ADDRESS'), ('THIRD', '3RD', 'ADDRESS'), ('FOURTH', '4TH', 'ADDRESS'), ('FIFTH', '5TH', 'ADDRESS'), ('SIXTH', '6TH', 'ADDRESS'), ('SEVENTH', '7TH', 'ADDRESS'), ('EIGHTH', '8TH', 'ADDRESS'), ('NINTH', '9TH', 'ADDRESS'), ('ELEVENTH','11TH','ADDRESS');

SELECT * FROM dictionary;

--alter the samples table to hold standardized fname and address values
alter table samples add column fname_std string, address_std string;

SELECT * FROM samples;
--apply the standardized fname to the new fname_std column
update samples s set fname_std = d.name from (
    select
        a.cust_key,
        coalesce(b.term, a.fname) name
    from samples a
    left join dictionary b
    on a.fname = b.token and context = 'NAME') d
where s.cust_key = d.cust_key and s.fname != d.name;


--see the records that were updated
select * from samples where fname != fname_std;

/*
	The query below tokenizes the address field from the samples table and then performs lookups into the dictionary table.
	It then puts the tokens back together and saves the new standardized address values in a temp table. Note we use the unique
	CUST_KEY as there may exist duplicate ADD_KEYS (such in cases of households)
*/

select 
  cust_key,
  to_json(array_agg(v) within group (order by idx asc)) x 
  from (
        with tokenized as (
            select 
                t.index,
                t.value,
                s.cust_key
            from samples s, lateral split_to_table(s.address, ' ') t
                where value > ''
          )
         select
            t.cust_key,
            t.index idx,
            coalesce(d.term, t.value) v
         from tokenized t
            left join dictionary d
            on t.value = d.token and d.context = 'ADDRESS'
            order by 1,2
        )
        group by cust_key;

create or replace temp table standard_address as (
select 
    cust_key,
    trim(REGEXP_REPLACE(x,'[^A-Z0-9]+', ' ')) address from (
select 
  cust_key,
  to_json(array_agg(v) within group (order by idx asc)) x from(

with tokenized as (
    select 
        t.index,
        t.value,
        s.cust_key
    from samples s, lateral split_to_table(s.address, ' ') t
        where value > ''
  )
select
    t.cust_key,
    t.index idx,
    coalesce(d.term, t.value) v
--      ,d.term dt, t.value
from tokenized t
left join dictionary d
on t.value = d.token and d.context = 'ADDRESS'
order by 1,2
  )
group by cust_key));

select * from standard_address limit 20;


--Join the updated address values back into our samples table
update samples s set s.address_std = a.address from 
    standard_address a 
    where s.cust_key = a.cust_key and  s.address != a.address;

select * from samples where editdistance(address,address_std)>1;
