WITH tile_visits_and_scenes AS (
    SELECT
        session_id,
        tile_id,
        visit_at,
        visit_at_dt,
        wallet_id,
        scene_hash,
        tile_id_base,
        tile_visit_secs,
        ROW_NUMBER() OVER (
            PARTITION BY session_id ORDER BY visit_at
        )
        - ROW_NUMBER() OVER (
            PARTITION BY
                scene_hash, session_id
            ORDER BY visit_at, scene_hash
        ) AS scene_session_group
    FROM
        {{ 'scene_wallet_movement' }}
    ORDER BY session_id ASC, visit_at ASC
),

agg_scene_visits AS (
    SELECT
        wallet_id,
        session_id,
        scene_session_group,
        tile_id_base,
        SUM(tile_visit_secs) AS scene_visit_secs,
        MIN(visit_at) AS visit_scene_at,
        MIN(visit_at_dt) AS visit_scene_at_dt
    FROM tile_visits_and_scenes
    GROUP BY 1, 2, 3, 4
    ORDER BY visit_scene_at ASC
),

agg_scene_visits_capped AS (
    SELECT
        wallet_id,
        session_id,
        tile_id_base,
        IFF(
            scene_visit_secs > 45 * 60,
            45 * 60,
            scene_visit_secs
        ) AS scene_visit_secs,
        visit_scene_at,
        visit_scene_at_dt,
        DATEADD(second, scene_visit_secs, visit_scene_at) AS left_scene_at
    FROM
        agg_scene_visits
    WHERE scene_visit_secs >= 5
)

SELECT
    wallet_id,
    tile_id_base,
    DATE_TRUNC('MONTH', DATEADD(DAY, -1, CURRENT_DATE)) AS start_considered_dt,
    DATEADD(DAY, -1, CURRENT_DATE) AS end_considered_dt,
    COUNT(DISTINCT(session_id)) AS sessions_visited,
    COUNT(*) AS visits,
    SUM(scene_visit_secs) AS acc_stay_secs,
    MIN(visit_scene_at) AS first_seen_at,
    MAX(left_scene_at) AS last_seen_at,
    DATEDIFF(DAYS, MIN(visit_scene_at), MAX(left_scene_at)) AS scene_retention,
    COUNT(DISTINCT visit_scene_at::DATE) AS days_visited,
    AVG(scene_visit_secs) AS avg_scene_stay
FROM agg_scene_visits_capped
WHERE visit_scene_at >= start_considered_dt
    AND visit_scene_at <= end_considered_dt
GROUP BY 1, 2, 3, 4
