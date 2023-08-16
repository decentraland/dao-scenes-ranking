WITH current_scene_metadata AS (
    SELECT
        tile_id_base,
        type,
        current_tile_owner,
        scene_title
    FROM {{ 'rel_scenes_tiles' }}
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY tile_id_base ORDER BY deployed_at DESC
        ) = 1
)


SELECT
    svd.tile_id_base,
    cs.current_tile_owner,
    cs.scene_title,
    svd.start_considered_dt,
    svd.end_considered_dt,
    SUM(svd.visits) AS visits,
    COUNT(DISTINCT(svd.wallet_id)) AS unique_visitors,
    SUM(svd.acc_stay_secs) / SUM(svd.visits) AS avg_stay_secs,
    (SUM(svd.acc_stay_secs) / 60) / SUM(svd.visits) AS avg_stay_mins,
    AVG(svd.scene_retention) AS avg_scene_retention,
    AVG(svd.days_visited) AS avg_days_returned,
    SUM(svd.visits) / 30 AS avg_daily_visits,
    COUNT(DISTINCT svd.wallet_id) / 30 AS avg_daily_unique_visitors
FROM {{ 'scene_wallet_visit_duration' }} AS svd
LEFT JOIN current_scene_metadata AS cs ON svd.tile_id_base = cs.tile_id_base
WHERE cs.type NOT IN ('road', 'plaza')
GROUP BY
    1, 2, 3, 4, 5
HAVING SUM(visits) > 1
