

--off and failed stat

data = LOAD 'Tingting/SensorPerf/twoweekdatapull.txt' USING PigStorage(',') AS 
(name:chararray,d:int, t1:chararray, t2:chararray, status:chararray); 

data2 = FOREACH data GENERATE name AS name:chararray, d AS date, (status=='off'?1:0) AS off:int, (status=='failed'?1:0) AS failed:int;

data_grp = GROUP data2 BY name;

status = FOREACH data_grp GENERATE group AS name:chararray, MAX(data2.date) AS date, AVG(data2.off) AS off:double, AVG(data2.failed) AS failed:double;


--vl and vc stat

err = LOAD 'Tingting/pca/vlvc' USING PigStorage(',') AS 
(vl:double, vc:double, name:chararray); 

err_grp = GROUP err BY name;

error = FOREACH err_grp GENERATE group AS name:chararray, AVG(err.vl) AS vl:double, AVG(err.vc) AS vc:double;

combined = JOIN status BY name FULL, error BY name;

combined1 = FOREACH combined GENERATE status::name AS name, status::date AS date, status::off AS off, status::failed AS failed, error::vl AS vl, error::vc AS vc;


--avg abs(diff) stat

diff = LOAD 'Tingting/pca/diff' USING PigStorage(',') AS 
(name:chararray, d:chararray, t:chararray, volume:int, occ:double, speed:double, cd:chararray, sd:chararray); 

diff2 = FILTER diff BY (cd!='null');

diff3 = FOREACH diff2 GENERATE name AS name, ABS((double)cd) AS cd, ABS((double)sd) AS sd;

diff_grp = GROUP diff3 BY name;

diffs = FOREACH diff_grp GENERATE group AS name:chararray, AVG(diff3.cd) AS cd:double, AVG(diff3.sd) AS sd:double;

combined2 = JOIN combined1 BY name FULL, diffs BY name;


--schema: name, date, off%, fail%, vlerror%, vcerror%, avg abs cd, avg abs sd.

combineall = FOREACH combined2 GENERATE combined1::name AS name, combined1::date AS date, combined1::off AS off:double, combined1::failed AS fail:double, combined1::vl AS vl:double, combined1::vc AS vc:double, diffs::cd AS cd:double, diffs::sd AS sd:double;


final = FILTER combineall BY (off+fail<1);


store final into 'Tingting/pca/error' USING PigStorage(',');
