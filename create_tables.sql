-- create carrier table
create table if not exists carrier (
    id integer primary key,
    carrier varchar(10),
    carrier_name varchar(100)
);

-- create airport table
create table if not exists airport (
    id integer primary key,
    airport varchar(10),
    airport_name varchar(100),
    city varchar(100),
    state varchar(10)
);

-- create date table
create table if not exists date (
    id integer,
    date date,
    year integer,
    month integer
);

-- create flights table
create table if not exists flights (
    id integer,
    date_id integer,
    carrier_id integer,
    airport_id integer,
    num_flights integer,
    num_delay integer,
    num_cancelled integer,
    num_diverted integer,
    total_delay_time integer,
    carrier_delay integer,
    weather_delay integer,
    nas_delay integer,
    security_delay integer,
    late_aircraft_delay integer
);

