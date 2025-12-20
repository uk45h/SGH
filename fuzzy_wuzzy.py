use sgh.public;

create or replace function fuzzy_ratio(text1 string, test2 string)
returns int
language python
runtime_version = '3.10'
packages = ('fuzzywuzzy','python-levenshtein')
handler = 'fuzzywuzzy'
as
$$
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
def fuzzywuzzy(text1,text2):
    ratio = fuzz.ratio(text1,text2)
    return ratio
$$;

create or replace function fuzzy_partial_ratio(text1 string, test2 string)
returns int
language python
runtime_version = '3.10'
packages = ('fuzzywuzzy','python-levenshtein')
handler = 'fuzzywuzzy'
as
$$
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
def fuzzywuzzy(text1,text2):
    ratio = fuzz.partial_ratio(text1,text2)
    return ratio
$$;

create or replace function fuzzy_token_sort_ratio(text1 string, test2 string)
returns int
language python
runtime_version = '3.10'
packages = ('fuzzywuzzy','python-levenshtein')
handler = 'fuzzywuzzy'
as
$$
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
def fuzzywuzzy(text1,text2):
    ratio = fuzz.token_sort_ratio(text1,text2)
    return ratio
$$;

select fuzzy_ratio('Łukasz Leszewski S.','Łukasz Leszewski');
select fuzzy_partial_ratio('Łukasz Leszewski S.','Łukasz Leszewski');
select fuzzy_partial_ratio('Łukasz Leszewski','Łukasz Leszewski S.');

select fuzzy_ratio('Łukasz Leszewski S.','Leszewski Łukasz');
select fuzzy_partial_ratio('Łukasz Leszewski S.','Leszewski Łukasz');
select fuzzy_token_sort_ratio('Łukasz Leszewski S.','Leszewski Łukasz');
select fuzzy_token_sort_ratio('Łukasz Leszewski S.','Leszewski S. Łukasz');

