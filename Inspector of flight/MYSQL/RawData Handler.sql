DROP DATABASE IF EXISTS flights1;
CREATE DATABASE flight1;
USE flights1;

CREATE TABLE flights1.flog LIKE  hw01.st;
INSERT INTO flights1.flog  SELECT * FROM  hw01.st;
UPDATE flights1.flog SET flog.`callsign` = REPLACE(`callsign`, ' ', '');


DROP VIEW   IF EXISTS flights_temp;
CREATE VIEW flights_temp AS SELECT t1.*, CONCAT(first,'_', date) AS idf FROM 
  (SELECT t.*,
         FIRST_VALUE(t.ic_ca)  OVER w AS 'first',
         FIRST_VALUE(t.d) OVER w AS 'date'#,
         #FIRST_VALUE(t.baroaltitude) OVER w AS 'fbar',
         #LAST_VALUE(t.baroaltitude)   OVER w AS 'lbar'#,
         #MIN(t.baroaltitude) OVER W AS mnb
         #NTH_VALUE(t.idf, 2) OVER w AS 'second',
        # NTH_VALUE(t.idf, 4) OVER w AS 'fourth'
  FROM (SELECT f.*,TIME(`time`) AS t, DATE(`time`) AS d, CONCAT(icao24,'_', callsign)  AS ic_ca FROM flog f
        WHERE callsign IS NOT NULL AND   callsign!=''  AND lastposupdate <20 AND lastcontact <20) AS t
  WINDOW w AS (PARTITION BY t.callsign ORDER BY t.time RANGE INTERVAL 2 HOUR PRECEDING)) AS t1 ORDER BY t1.time ASC;




#SELECT idf, org_country as cntr, lat, lon, baroaltitude as bh, velocity as vel, heading as he, vertrate as ver, geoaltitude as gh, time as dt  FROM flights_temp;

DROP VIEW  IF EXISTS for_features;
CREATE VIEW for_features AS SELECT t1.* FROM 
  (SELECT  idf, org_country as cntr, lat, lon, baroaltitude as bh, velocity as vel, heading as he, vertrate as ver, geoaltitude as gh, time as dt  
    FROM flights_temp) AS t1 ORDER BY t1.dt ASC;



DROP TEMPORARY TABLE IF EXISTS th;
CREATE TEMPORARY TABLE th AS SELECT DISTINCT t2.* FROM (
SELECT t.idf,
        FIRST_VALUE(t.dt) OVER w AS 'fdt',
        LAST_VALUE(t.dt)  OVER w AS 'ldt',  
        FIRST_VALUE(t.bh) OVER w AS 'fbh',      
        NTH_VALUE(t.bh, 10) OVER w AS '10bh',
        NTH_VALUE(t.bh, 30) OVER w AS '30bh',
        NTH_VALUE(t.bh, 70) OVER w AS '70bh',
        LAST_VALUE(t.bh)  OVER w AS 'lbh'
       # MIN(t.baroaltitude) OVER w AS mnb
FROM for_features  AS  t
WINDOW w AS (PARTITION BY t.idf ORDER BY t.dt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  ORDER BY t.dt ASC) AS t2 GROUP BY t2.fdt;

SELECT * FROM th;

CREATE TABLE temp_h LIKE th;
INSERT INTO temp_h  SELECT * FROM th;

DROP TEMPORARY TABLE IF EXISTS tv;
CREATE TEMPORARY TABLE tv AS SELECT DISTINCT t2.* FROM (
SELECT t.idf,
        FIRST_VALUE(t.dt) OVER w AS 'fdt',
        FIRST_VALUE(t.vel) OVER w AS 'fv',      
        NTH_VALUE(t.vel, 10) OVER w AS '10v',
        NTH_VALUE(t.vel, 30) OVER w AS '30v',
        NTH_VALUE(t.vel, 70) OVER w AS '70v',
        LAST_VALUE(t.vel)  OVER w AS 'lv'
FROM for_features  AS  t
WINDOW w AS (PARTITION BY t.idf ORDER BY t.dt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  ORDER BY t.dt ASC) AS t2 GROUP BY t2.fdt;

SELECT * FROM tv;

CREATE TABLE temp_vel LIKE tv;
INSERT INTO temp_vel  SELECT * FROM tv;

DROP TEMPORARY TABLE IF EXISTS tlat;
CREATE TEMPORARY TABLE tlat AS SELECT DISTINCT t2.* FROM (
SELECT t.idf,
        FIRST_VALUE(t.dt) OVER w AS 'fdt',
        FIRST_VALUE(t.lat) OVER w AS 'flat',
        NTH_VALUE(t.lat, 10) OVER w AS '10lat',
        NTH_VALUE(t.lat, 30) OVER w AS '30lat',
        NTH_VALUE(t.lat, 70) OVER w AS '70lat',
        LAST_VALUE(t.lat)  OVER w AS 'llat'
FROM for_features  AS  t
WINDOW w AS (PARTITION BY t.idf ORDER BY t.dt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  ORDER BY t.dt ASC) AS t2 GROUP BY t2.fdt;

SELECT * FROM tlat;

#CREATE TABLE temp_coord LIKE tlat;
#INSERT INTO temp_coord  SELECT * FROM tlat;

DROP TEMPORARY TABLE IF EXISTS tlon;
CREATE TEMPORARY TABLE tlon AS SELECT DISTINCT t2.* FROM (
SELECT t.idf,
        FIRST_VALUE(t.dt) OVER w AS 'fdt',
        FIRST_VALUE(t.lon) OVER w AS 'flon',
        NTH_VALUE(t.lon, 10) OVER w AS '10lon',
        NTH_VALUE(t.lon, 30) OVER w AS '30lon',
        NTH_VALUE(t.lon, 70) OVER w AS '70lon',
        LAST_VALUE(t.lon)  OVER w AS 'llon'
FROM for_features  AS  t
WINDOW w AS (PARTITION BY t.idf ORDER BY t.dt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  ORDER BY t.dt ASC) AS t2 GROUP BY t2.fdt;

SELECT * FROM tlon;

#CREATE TABLE temp_lon LIKE tlon;
#INSERT INTO temp_lon  SELECT * FROM tlon;

DROP TEMPORARY TABLE IF EXISTS tc;
CREATE TEMPORARY TABLE tc AS SELECT * FROM
  (SELECT * FROM tlon) AS t1
INNER JOIN 
  (SELECT  t2.idf AS idf2, t2.flat, t2.`10lat`,t2.`30lat`,t2.`70lat`, t2.llat FROM tlat t2)  AS t2
ON t1.idf = t2.idf2;

CREATE TABLE temp_coord LIKE tc;
INSERT INTO temp_coord  SELECT * FROM tc;

DROP TEMPORARY TABLE IF EXISTS rtemp;
CREATE TEMPORARY TABLE rtemp AS SELECT * FROM
  (SELECT * FROM temp_h) AS t1
INNER JOIN 
  (SELECT  t2.idf AS idf2, t2.flat, t2.`10lat`,t2.`30lat`,t2.`70lat`, t2.llat, t2.flon, t2.`10lon`,t2.`30lon`, t2.`70lon`, t2.llon FROM temp_coord t2)  AS t2
ON t1.idf = t2.idf2;

SELECT * FROM rtemp;

DROP TEMPORARY TABLE IF EXISTS rtemp2;
CREATE TEMPORARY TABLE rtemp2 AS SELECT * FROM
  (SELECT * FROM rtemp) AS t1
INNER JOIN 
  (SELECT  t2.idf AS idf3, t2.fv, t2.`10v`,t2.`30v`,t2.`70v`, t2.lv FROM temp_vel t2)  AS t2
ON t1.idf = t2.idf3;

SELECT * FROM rtemp2;


DROP TEMPORARY TABLE IF EXISTS fe;
CREATE TEMPORARY TABLE fe AS SELECT * FROM
  (SELECT idf, fdt, ldt, fv, lv, fbh, `10bh`, `30bh`,`70bh`, lbh,  `10v`, `30v`,`70v`,  flat, `10lat`, `30lat`,`70lat`, llat, flon, `10lon`,`30lon`, `70lon`, llon FROM rtemp2 f) AS t;

DROP  TABLE IF EXISTS features;
CREATE TABLE features LIKE fe;
INSERT INTO features  SELECT * FROM fe;

SET @shlat= 37.410372;  
SET @shlon = 55.980681;

SET @dlat= 37.906111000000;  
SET @dlon = 55.408611000000;

SET @vlat= 37.292100000000;  
SET @vlon = 55.603150000000;

SET @glat= 38.1181793 ;  
SET @glon = 55.5614395 ;

#CREATE TEMPORARY TABLE temp_felab AS (SELECT idf, fdt, ldt, fv, lv, fbh, lbh, llon,llat,  flon, flat,`10bh`, 
#  `30bh`,`70bh`, `10v`, `30v`,`70v`, `10lat`, `30lat`,`70lat`, `10lon`, `30lon`,`70lon`, POW((f.`70lon`-@shlon),2)+POW((f.`70lat`-@shlat),2) AS dsh,
#  POW((f.`70lon`-@dlon),2)+POW((f.`70lat`-@dlat),2) AS dd, POW((f.`70lon`-@vlon),2)+POW((f.`70lat`-@vlat),2) AS dv, 
#  POW((f.`70lon`-@glon),2)+POW((f.`70lat`-@glat),2) AS dg,

DROP TEMPORARY TABLE IF EXISTS temp_felab;
CREATE TEMPORARY TABLE temp_felab AS (SELECT idf, fdt, ldt, fv, lv, fbh, lbh, llon,llat,  flon, flat,`10bh`, 
  `30bh`,`70bh`, `10v`, `30v`,`70v`, `10lat`, `30lat`,`70lat`, `10lon`, `30lon`,`70lon`, POW((f.llon-@shlon),2)+POW((f.llat-@shlat),2) AS dsh,
  POW((f.llon-@dlon),2)+POW((f.llat-@dlat),2) AS dd, POW((f.llon-@vlon),2)+POW((f.llat-@vlat),2) AS dv, 
  POW((f.llon-@glon),2)+POW((f.llat-@glat),2) AS dg,
CASE
    WHEN lbh < 1100 AND  POW((f.llon-@shlon),2)+POW((f.llat-@shlat),2)<0.07
        THEN 'SVO'
    WHEN lbh < 1100 AND  POW((f.llon-@dlon),2)+POW((f.llat-@dlat),2)<0.07
        THEN 'DME'
    WHEN lbh < 1100 AND  POW((f.llon-@vlon),2)+POW((f.llat-@vlat),2)<0.07
        THEN 'VKO'
    WHEN lbh < 1100 AND  POW((f.llon-@glon),2)+POW((f.llat-@glat),2)<0.07
        THEN 'ZIA'
    WHEN (lbh > 4000 AND fbh<1100) OR  (fv<lv) AND (lbh-fbh)>2000
        THEN 'Takeoff'
    WHEN fbh > 8000 AND lbh>8000 AND ABS(lbh-fbh)<2000 AND ABS((lv-fv))/fv<0.2
        THEN 'Transit'
    WHEN (lbh > fbh) AND (lv > fv)
        THEN 'Takeoff?'
    ELSE 'Not enough data'
END AS cat FROM features as f);

SELECT GROUP_CONCAT(column_name SEPARATOR ',\n') FROM information_schema.columns WHERE table_schema = 'flights' AND table_name = 'felab';

SELECT idf, SUBSTRING_INDEX(idf, '_', 1) AS icao24, 
  SUBSTRING_INDEX(SUBSTRING_INDEX(idf, '_', 2) , '_',-1) AS callsign, fdt,ldt,
  fbh, fv, lv, lbh, llat, llon, cat,   flon, flat, dd, dg, dsh, dv 
FROM temp_felab;

DROP  TABLE IF EXISTS felab;
CREATE TABLE felab LIKE temp_felab;
INSERT INTO felab  SELECT * FROM temp_felab WHERE cat !='Takeoff';

SELECT * FROM felab; 

SELECT cat, DATE(fdt) AS da, count(*) FROM felab GROUP BY cat, da ORDER BY da ASC;

