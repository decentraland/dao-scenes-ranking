SELECT
    s.scene_title,
    s.tile_id_base,
    s.current_tile_owner,
    s.start_considered_dt,
    s.end_considered_dt,
    s.visits,
    s.unique_visitors,
    s.avg_stay_secs,
    s.avg_stay_mins,
    s.avg_scene_retention,
    s.avg_days_returned,
    s.avg_daily_visits,
    s.avg_daily_unique_visitors
FROM {{ 'scene_visit_duration' }} AS s
QUALIFY row_number() OVER(
    PARTITION BY s.start_considered_dt, s.end_considered_dt
    ORDER BY s.avg_daily_visits DESC) <= 100
ORDER BY s.avg_daily_visits DESC
