define VAR datafu.pig.stats.VAR();


data = LOAD 'Tingting/pca/error' USING PigStorage(',') AS 
(name:chararray, date: chararray, off:double, fail:double, vl:double, vc:double, cd:double, sd:double); 

offsd = FOREACH (GROUP data ALL) GENERATE SQRT(VAR(data.off)) AS sd;

failsd = FOREACH (GROUP data ALL) GENERATE SQRT(VAR(data.fail)) AS sd;

vlsd = FOREACH (GROUP data ALL) GENERATE SQRT(VAR(data.vl)) AS sd;

vcsd = FOREACH (GROUP data ALL) GENERATE SQRT(VAR(data.vc)) AS sd;

cdsd = FOREACH (GROUP data ALL) GENERATE SQRT(VAR(data.cd)) AS sd;

sdsd = FOREACH (GROUP data ALL) GENERATE SQRT(VAR(data.sd)) AS sd;



offmean = FOREACH (GROUP data ALL) GENERATE AVG(data.off) AS mean;

failmean = FOREACH (GROUP data ALL) GENERATE AVG(data.fail) AS mean;

vlmean = FOREACH (GROUP data ALL) GENERATE AVG(data.vl) AS mean;

vcmean = FOREACH (GROUP data ALL) GENERATE AVG(data.vc) AS mean;

cdmean = FOREACH (GROUP data ALL) GENERATE AVG(data.cd) AS mean;

sdmean = FOREACH (GROUP data ALL) GENERATE AVG(data.sd) AS mean;




scaled = FOREACH data GENERATE name, date, ((off-offmean.mean)/offsd.sd) AS off, ((fail-failmean.mean)/failsd.sd) AS fail, ((vl-vlmean.mean)/vlsd.sd) AS vl,((vc-vcmean.mean)/vcsd.sd) AS vc, ((cd-cdmean.mean)/cdsd.sd) AS cd, ((sd-sdmean.mean)/sdsd.sd) AS sd;

pc = LOAD 'Tingting/pca/pc.txt' USING PigStorage(',') AS 
(off:double, fail:double, vl:double, vc:double, cd:double, sd:double); 


score = FOREACH scaled GENERATE name, date, off*pc.off+fail*pc.fail+vl*pc.vl+vc*pc.vc+cd*pc.cd+sd*pc.sd;

STORE score INTO 'Tingting/pca/score' USING PigStorage(',');