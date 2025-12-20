use sgh.public;

--claude-4-sonnet
create or replace table baza_klientow_llm_llama as 
    select 
        miasto_zameldowania,
        ulica_zamieszkania,
        snowflake.cortex.complete('llama3.1-70b',
            'If this is not the correct address in Poland, offer the correct address in the format city: correct city name and street: correct street name. Dont add any additional description - keep the answer short. Data - city: '||miasto_zameldowania||' street: '||ulica_zamieszkania) as llm,
        REGEXP_SUBSTR(llm,'city:\\s*([^,]+),?\\s*street', 1, 1, 'i', 1) as miasto,
        REGEXP_SUBSTR(llm,'street:\\s*([^,]+)', 1, 1, 'i', 1) as street 
    from baza_klientow;

select * from baza_klientow_llm_llama limit 10;

