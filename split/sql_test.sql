delete 
from test.sbtest1 limit 300;

select * 
from test.sbtest1 limit 300;

explain analyze select * 
from test.sbtest1 s1,test.sbtest2 s2,test.sbtest3 s3,test.sbtest1 s4 
where s1.id=s2.id and s1.id=s3.id and s2.id=s3.id and s1.id=s4.id and s1.k='5014614';

