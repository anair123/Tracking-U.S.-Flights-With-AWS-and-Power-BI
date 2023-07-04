-- 1. Which airports had the most flights in 2022?
select a.airport_name,
    sum(f.num_flights) as num_flights
from flights f
join airport a
    on f.airport_id = a.id
join date d
    on f.date_id = d.id
where d.year = 2022
group by a.airport_name
order by num_flights desc
limit 5;

-- 2. What type of delay contributes most to total delay in each year?
-- total delay per year
with yearly_delay as (
SELECT d.year, 
    sum(carrier_delay) as carrier_delay,
    sum(weather_delay) as weather_delay,
    sum(nas_delay) as nas_delay,
    sum(security_delay) as security_delay,
    sum(late_aircraft_delay) as late_aircraft_delay
FROM flights f
join date d
    on f.date_id = d.id
group by d.year)

select year,
    case 
        when carrier_delay>weather_delay and carrier_delay>security_delay and carrier_delay> late_aircraft_delay and carrier_delay >nas_delay then 'carrier delay'
        when weather_delay>carrier_delay and weather_delay>security_delay and weather_delay> late_aircraft_delay and weather_delay >nas_delay then 'weather delay'
        when security_delay>carrier_delay and security_delay>weather_delay and security_delay> late_aircraft_delay and security_delay >nas_delay then 'security delay'
        when late_aircraft_delay>carrier_delay and late_aircraft_delay>weather_delay and late_aircraft_delay> security_delay and late_aircraft_delay >nas_delay then 'late aircraft delay'
        when nas_delay>weather_delay and nas_delay>security_delay and nas_delay> late_aircraft_delay and nas_delay >carrier_delay then 'nas delay' END
from yearly_delay
where year >= 2019
order by year;

-- 3. What is the percent change in delay for the JFK airport on a year-to-year basis?

-- today delay per year in JFK
with yearly_delay as (
select year, 
    sum(total_delay_time) as total_delay_time
from flights f 
join date d
    on f.date_id = d.id
join airport a
    on f.airport_id = a.id
where airport_name = 'John F. Kennedy International'
group by year),

-- previous year's delay for each year
previous_delay as (
select year,
    total_delay_time,
    lag(total_delay_time,1) over(order by year) as previous_delay
from yearly_delay
order by year)

select year, 
    ROUND((total_delay_time-previous_delay)/cast(previous_delay as float) *100,2) as pct_change
from previous_delay;