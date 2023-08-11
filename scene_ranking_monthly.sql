WITH scene_rankings AS (
    SELECT
        tile_id_base,
        scene_title,
        current_tile_owner,
        start_considered_dt,
        end_considered_dt,
        avg_daily_visits,
        avg_daily_unique_visitors,
        avg_days_returned,
        avg_stay_mins,
        avg_scene_retention,
        row_number() OVER(PARTITION BY start_considered_dt, end_considered_dt
            ORDER BY avg_daily_visits DESC, avg_daily_unique_visitors DESC) AS dv_ranking,
        row_number() OVER(PARTITION BY start_considered_dt, end_considered_dt
            ORDER BY avg_daily_unique_visitors DESC, avg_daily_visits DESC) AS duv_ranking,
        row_number() OVER(PARTITION BY start_considered_dt, end_considered_dt
            ORDER BY avg_days_returned DESC, avg_daily_visits DESC) AS ard_ranking,
        row_number() OVER(PARTITION BY start_considered_dt, end_considered_dt
            ORDER BY avg_stay_mins DESC, avg_daily_visits DESC) AS asm_ranking,
        row_number() OVER(PARTITION BY start_considered_dt, end_considered_dt
            ORDER BY avg_scene_retention DESC, avg_daily_visits DESC) AS asr_ranking
    FROM {{ 'top_scenes_monthly' }}
    ORDER BY dv_ranking
),

weights AS (
    SELECT
        weight,
        row_number() OVER(ORDER BY weight DESC) AS ranking
    FROM {{ 'scene_ranking_weights' }}
)

SELECT
    r.*,
    w1.weight AS dv_score,
    w2.weight AS duv_score,
    w3.weight AS ard_score,
    w4.weight AS asm_score,
    w5.weight AS asr_score,
    dv_score + duv_score + ard_score + asm_score + asr_score AS final_score,
    row_number() OVER(PARTITION BY r.start_considered_dt, r.end_considered_dt
        ORDER BY final_score DESC) AS final_ranking
FROM scene_rankings AS r
INNER JOIN weights AS w1 ON w1.ranking = r.dv_ranking
INNER JOIN weights AS w2 ON w2.ranking = r.duv_ranking
INNER JOIN weights AS w3 ON w3.ranking = r.ard_ranking
INNER JOIN weights AS w4 ON w4.ranking = r.asm_ranking
INNER JOIN weights AS w5 ON w5.ranking = r.asr_ranking
ORDER BY final_score DESC
