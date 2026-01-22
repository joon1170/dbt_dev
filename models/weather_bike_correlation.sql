WITH CTE AS (
    select
    t.*,
    w.*

    from {{ ref('trip_fact') }} t
    left join {{ ref('daily_weather') }} w 
    on t.TRIP_DATE = w.DAILY_WEATHER
    limit 20
)

select * from CTE