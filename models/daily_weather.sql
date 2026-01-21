WITH daily_weather AS (
    select
    date(time) as daily_weather,
    clouds,
    humidity,
    pressure,
    temp,
    weather
    from {{ source('demo', 'weather') }}
),

daily_weather_agg AS (
    select
    daily_weather,
    weather,
    round(avg(clouds), 2) as avg_clouds,
    round(avg(humidity), 2) as avg_humidity,
    round(avg(pressure), 2) as avg_pressure,
    round(avg(temp), 2) as avg_temp
    from daily_weather
    group by daily_weather, weather
    qualify ROW_NUMBER() OVER (PARTITION BY daily_weather ORDER BY count(weather) desc) = 1
    order by daily_weather

)

select * from daily_weather_agg