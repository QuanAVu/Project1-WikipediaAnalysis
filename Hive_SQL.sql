-- Quan Vu, All SQL queries needed for Project1 in Hive 
-- The queries are written in a format that works for Hive beeline
-- so it is a little messy and hard to read.

-- Tables created:

CREATE TABLE PAGE_VIEW
(DOMAIN_CODE STRING, PAGE_TITLE STRING, COUNT_VIEWS INT, TOTAL_RESPONSE_SIZE INT)
ROW FORMAT DELIMITED	-- Each line represents a record
FIELDS TERMINATED BY ' '; -- Each element for each column is separated by a space
-- Load 
LOAD DATA LOCAL INPATH '/home/quanvu/Project1-Data/pageViews/octo20-fullday' INTO TABLE PAGE_VIEW;

/* Three different tables for three countries*/
-- Rush hour table for UK 
-- For US and AU tables just change the table name and the path to the file accordingly 
CREATE TABLE PAGE_VIEW_UK
(DOMAIN_CODE STRING, PAGE_TITLE STRING, COUNT_VIEWS INT, TOTAL_RESPONSE_SIZE INT)
ROW FORMAT DELIMITED	
FIELDS TERMINATED BY ' ';

LOAD DATA LOCAL INPATH '/home/quanvu/Project1-Data/pageViews/octo20-UK' INTO TABLE PAGE_VIEW_UK;


CREATE TABLE CLICKSTREAM
(ORIGIN STRING, INTERNAL STRING, RELATION STRING, CLICKS INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';
-- Load local data into table 
LOAD DATA LOCAL INPATH '/home/quanvu/Project1-Data/clickstream-enwiki-2020-09.tsv' INTO TABLE clickstream;

-------------------------------------------------------------------------------------------------------------------

-- Query 1
/*	Get the top 10 highest viewed English wikipedia pages.
	First sum is the summing of count views from all 24 hours
	page_view files.
	Second sum is summing matching articles from both en and en.m */
-- Can add the query result onto HDFS: 
INSERT OVERWRITE DIRECTORY '/user/hive/output/Query1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '  '
SELECT NewTable.PAGE_TITLE, SUM(NewTable.SUM_VIEWS) AS TOTAL_VIEWS
FROM (SELECT DOMAIN_CODE, PAGE_TITLE, SUM(COUNT_VIEWS) AS SUM_VIEWS
      FROM PAGE_VIEW
      WHERE DOMAIN_CODE= 'en' OR DOMAIN_CODE= 'en.m'
      GROUP BY DOMAIN_CODE, PAGE_TITLE) AS NewTable
GROUP BY NewTable.PAGE_TITLE
ORDER BY TOTAL_VIEWS DESC
LIMIT 10;


-- Query 2
/*	Get the top 10 English wikipedia articles that have internal links
	that were clicked the most (highest fraction).*/
INSERT OVERWRITE DIRECTORY '/user/hive/output/Query2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '  '
SELECT Table2.origin AS referrer, Table2.internal AS requested, ROUND(Table2.clicks/Table1.total1, 2) AS fraction
FROM (SELECT origin, SUM(clicks) AS total1 FROM CLICKSTREAM WHERE relation='link' GROUP BY origin ORDER BY total1 DESC LIMIT 1000) AS Table1, (SELECT * FROM CLICKSTREAM WHERE relation='link' ORDER BY clicks DESC LIMIT 1000) AS Table2
WHERE Table1.origin = Table2.origin
ORDER BY fraction DESC 
LIMIT 10;


-- Query 3
/* 	Get a series of articles starting from Hotel_California
	that have the highest fraction of clicks (the clicks number is not
	accurate because it is a combination of both clicks that are 
	from the chain and clicks from when user search for the last
	internal article as an original article)*/
INSERT OVERWRITE DIRECTORY '/user/hive/output/Query3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '  '
SELECT NewTable.referrer AS referrer, NewTable.requested AS requested1, CLICKSTREAM.internal AS requested2, ROUND(CLICKSTREAM.clicks/NewTable.total, 2) AS final_fraction
FROM CLICKSTREAM, (SELECT Table2.origin AS referrer, Table2.internal AS requested, Table1.total1 AS total, ROUND(Table2.clicks/Table1.total1, 2) AS fraction
FROM (SELECT origin, SUM(clicks) AS total1 FROM CLICKSTREAM WHERE origin='Hotel_California' AND relation='link' GROUP BY origin ORDER BY total1 DESC LIMIT 1000) AS Table1, (SELECT * FROM CLICKSTREAM WHERE origin='Hotel_California' AND relation='link' ORDER BY clicks DESC LIMIT 1000) AS Table2
WHERE Table1.origin = Table2.origin
ORDER BY fraction DESC LIMIT 1) AS NewTable 
WHERE NewTable.requested = CLICKSTREAM.origin
ORDER BY final_fraction DESC
LIMIT 10;


-- Query 4
/* 	Get the highest viewed English wikipedia article in October 20, 2020
	for US, UK, AU based on Internet rush hours of each country.
	We have three different queries for our three tables 
*/

-- Query to get max page view of articles for UK 
-- For queries on US and AU just change Page_Title to US or AU 
-- and the nested FROM clause to appropriate country
INSERT OVERWRITE DIRECTORY '/user/hive/output/Query4UK'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '  '

SELECT NewTable.PAGE_TITLE AS AU_PAGE_TITLE, SUM(NewTable.SUM_VIEWS) AS TOTAL_VIEWS
FROM (SELECT DOMAIN_CODE, PAGE_TITLE, SUM(COUNT_VIEWS) AS SUM_VIEWS
      FROM PAGE_VIEW_AU
      WHERE DOMAIN_CODE= 'en' OR DOMAIN_CODE= 'en.m'
      GROUP BY DOMAIN_CODE, PAGE_TITLE) AS NewTable
GROUP BY NewTable.PAGE_TITLE
ORDER BY TOTAL_VIEWS DESC
LIMIT 10;



-- Query 5 (4 steps and two different tables -- revision and AU page view tables)
-- TODO: Populate table with about 70 fields? -- maybe use mapreduce to down size the number of columns?
-- Find the average vandelized/revised page (for October 20,2020) by looking at the cumulative revision count (enwiki revision create only)
-- Once the page is found, pick a revision where the time of creation to the time it got reverted is short so we can find
-- the number of page views in that time. (before picking the revision, search for the revision that had been reverted by future revision ) 
-- Have to compare page_view and revision tables 

-- Look for the average number of revision for a page (in october 20, 2020) rounded to the nearest 10th deci
SELECT ROUND(AVG(page_revision_count), 1) AS AVERAGE_Page_REVISION
FROM revision 
WHERE event_entity = 'revision' AND event_timestamp LIKE '2020-10-20%';

-- Look for the page that has the revision count similar to the average revision count (about 9203 counts)
SELECT page_title, page_revision_count 
FROM revision 
WHERE event_timestamp LIKE '2020-10-20%' AND event_entity='revision' AND page_revision_count=9203 LIMIT 10;

-- Look for the revision on the average article that we found where this revision got reverted by a future revision 
-- this gives us the time when the revision was created and when it got reverted.
SELECT page_title, event_timestamp, revision_seconds_to_identity_revert 
FROM revision 
WHERE page_title='Enrique_Iglesias' AND event_entity='revision' AND revision_is_identity_reverted=true AND event_timestamp LIKE '2020-10-20%' LIMIT 5;

-- Grab the revision timestamp when it was created (2020-10-20 10:42 AM) and the hours in between when it got reverted (about 4 hours in our case)
-- Out page-views files that we take happen to be the same files we used for AU page view table. 
-- Look for the article (Enrique_Iglesias) and search for the total page views 
SELECT NewTable.PAGE_TITLE AS PAGE_TITLE, SUM(NewTable.SUM_VIEWS) AS USERS
FROM (SELECT DOMAIN_CODE, PAGE_TITLE, SUM(COUNT_VIEWS) AS SUM_VIEWS
      FROM PAGE_VIEW_AU
      WHERE DOMAIN_CODE= 'en' OR DOMAIN_CODE= 'en.m'
      GROUP BY DOMAIN_CODE, PAGE_TITLE) AS NewTable
WHERE NewTable.PAGE_TITLE = 'Enrique_Iglesias'
GROUP BY PAGE_TITLE;

-- Query 6 Use MapReduce??? -> Trying to find the average amount of US editors for enwiki



{"data":{"created_at":"2020-11-18T02:55:36.000Z","entities":{"hashtags":[{"start":12,"end":21,"tag":"OceanTwp"},{"start":101,"end":109,"tag":"traffic"}],"urls":[{"start":110,"end":133,"url":"https://t.co/LhFKfd3DbJ","expanded_url":"http://bit.ly/11xKLzq","display_url":"bit.ly/11xKLzq","status":200,"title":"Sigalert","description":"New York traffic reports. Real-time speeds, accidents, and traffic cameras. Check conditions on bridge and tunnel crossings, the LIE, the New Jersey Turnpike and other routes. Email or text traffic alerts on your personalized routes.","unwound_url":"https://www.sigalert.com/Map.asp?region=New+York"}]},"text":"Accident in #OceanTwp on Rt-18 NB approaching Deal Rd/Exit 11, stopped traffic back to Rt-66/Exit 10 #traffic https://t.co/LhFKfd3DbJ","id":"1328894599910227968","author_id":"1325862575762935811"},"includes":{"users":[{"id":"1325862575762935811","name":"Quan Vu","created_at":"2020-11-09T18:07:35.000Z","username":"QuanVu72601925"}]},"matching_rules":[{"id":1328891769409343489,"tag":""}]}
{"data":{"id":"1328896783448039424","author_id":"1325862575762935811","entities":{"hashtags":[{"start":12,"end":21,"tag":"OceanTwp"},{"start":101,"end":109,"tag":"traffic"}],"urls":[{"start":110,"end":133,"url":"https://t.co/LhFKfd3DbJ","expanded_url":"http://bit.ly/11xKLzq","display_url":"bit.ly/11xKLzq","status":200,"title":"Sigalert","description":"New York traffic reports. Real-time speeds, accidents, and traffic cameras. Check conditions on bridge and tunnel crossings, the LIE, the New Jersey Turnpike and other routes. Email or text traffic alerts on your personalized routes.","unwound_url":"https://www.sigalert.com/Map.asp?region=New+York"}]},"text":"Accident in #OceanTwp on Rt-19 NB approaching Deal Rd/Exit 11, stopped traffic back to Rt-66/Exit 10 #traffic https://t.co/LhFKfd3DbJ","created_at":"2020-11-18T03:04:17.000Z"},"includes":{"users":[{"name":"Quan Vu","username":"QuanVu72601925","created_at":"2020-11-09T18:07:35.000Z","id":"1325862575762935811"}]},"matching_rules":[{"id":1328891769409343489,"tag":""}]}
{"data":{"id":"1328897719390281730","author_id":"1325862575762935811","entities":{"hashtags":[{"start":12,"end":21,"tag":"OceanTwp"},{"start":101,"end":109,"tag":"traffic"}],"urls":[{"start":110,"end":133,"url":"https://t.co/LhFKfd3DbJ","expanded_url":"http://bit.ly/11xKLzq","display_url":"bit.ly/11xKLzq","status":200,"title":"Sigalert","description":"New York traffic reports. Real-time speeds, accidents, and traffic cameras. Check conditions on bridge and tunnel crossings, the LIE, the New Jersey Turnpike and other routes. Email or text traffic alerts on your personalized routes.","unwound_url":"https://www.sigalert.com/Map.asp?region=New+York"}]},"text":"Accident in #OceanTwp on Rt-20 NB approaching Deal Rd/Exit 11, stopped traffic back to Rt-66/Exit 10 #traffic https://t.co/LhFKfd3DbJ","created_at":"2020-11-18T03:08:00.000Z"},"includes":{"users":[{"name":"Quan Vu","username":"QuanVu72601925","created_at":"2020-11-09T18:07:35.000Z","id":"1325862575762935811"}]},"matching_rules":[{"id":1328891769409343489,"tag":""}]}
{"data":{"id":"1328933762818863107","entities":{"hashtags":[{"start":12,"end":21,"tag":"OceanTwp"},{"start":101,"end":109,"tag":"traffic"}],"urls":[{"start":110,"end":133,"url":"https://t.co/LhFKfcM2kb","expanded_url":"http://bit.ly/11xKLzq","display_url":"bit.ly/11xKLzq","status":200,"title":"Sigalert","description":"New York traffic reports. Real-time speeds, accidents, and traffic cameras. Check conditions on bridge and tunnel crossings, the LIE, the New Jersey Turnpike and other routes. Email or text traffic alerts on your personalized routes.","unwound_url":"https://www.sigalert.com/Map.asp?region=New+York"}]},"text":"Accident in #OceanTwp on Rt-19 NB approaching Deal Rd/Exit 11, stopped traffic back to Rt-66/Exit 10 #traffic https://t.co/LhFKfcM2kb","created_at":"2020-11-18T05:31:14.000Z","author_id":"1325862575762935811"},"includes":{"users":[{"name":"Quan Vu","created_at":"2020-11-09T18:07:35.000Z","username":"QuanVu72601925","id":"1325862575762935811"}]},"matching_rules":[{"id":1328891769409343489,"tag":""}]}
 
 

