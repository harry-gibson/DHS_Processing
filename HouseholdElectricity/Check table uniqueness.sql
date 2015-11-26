-- How many rows do we have in the table
SELECT count(*) from dhs_data_tables."RECH0" -- This matches the number read by FME except for survey 222 which isn't in the DB
SELECT count(distinct surveyid) from dhs_data_tables."RECH0"
-- Is it the same number of distinct values in the columns we presume to be unique?
SELECT count (*) from (select distinct surveyid, hhid from dhs_data_tables."RECH0") as blah
-- No! Hmm.

-- Same for the other table we wish to join
-- N in the RECH2 table matches the number of features in the source CSVs as reported by FME
SELECT count(*) from dhs_data_tables."RECH2" -- This matches the number read by FME exclu survey 222
SELECT count(distinct surveyid) from dhs_data_tables."RECH2"
SELECT count (*) from (select distinct surveyid, hhid from dhs_data_tables."RECH2") as blah

-- How does the n of results vary with inner / outer joins? 
SELECT count(*) 
from 
dhs_data_tables."RECH0" h0
LEFT JOIN 
dhs_data_tables."RECH2" h2
ON 
h0.HHID = h2.HHID
and 
h0.surveyid = h2.surveyid

-- So we have nonunique combinations of surveyid / hhid somewhere. Find out which surveys:
SELECT * from dhs_data_tables."RECH0" h0 
natural join 
(select surveyid, hhid, count(*) cnt from dhs_data_tables."RECH0" GROUP BY surveyid, hhid) dupes
where dupes.cnt > 1
order by surveyid
-- apart from two genuine duplicates in domininican republic 1991 (id 37) which we won't worry about
-- these are all in survey 156. that flipping India 1999 one. A single id but multiple surveys.

SELECT count(*) 
from 
dhs_data_tables."RECH0" h0
INNER JOIN 
dhs_data_tables."RECH2" h2
ON 
h0.HHID = h2.HHID
and 
h0.surveyid = h2.surveyid
where h0.surveyid != 156

SELECT count(*) from dhs_data_tables."RECH0" where surveyid =156