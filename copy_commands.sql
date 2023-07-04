-- copy data into carrier table
copy carrier
from 's3://flights-data-processed/carriers.csv'
iam_role 'iam_role'
ignoreheader 1
delimiter ',';

select * from carrier limit 5;

-- copy data into airport table
copy airport
from 's3://flights-data-processed/airports.csv'
iam_role 'iam_role'
ignoreheader 1
delimiter ',';

select * from airport limit 5;

-- copy data into date table
copy date
from 's3://flights-data-processed/date.csv'
iam_role 'iam_role'
ignoreheader 1
delimiter ',';

select * from date limit 5;

-- copy data into flights table
copy flights
from 's3://flights-data-processed/flights.csv'
iam_role 'iam_role'
ignoreheader 1
delimiter ',';

select * from flights order by id limit 5;