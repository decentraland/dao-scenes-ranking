WITH duplicate_owners_removed AS (
    SELECT *
    FROM {{ 'scene_ranking_monthly' }}
    QUALIFY row_number() OVER (
            PARTITION BY current_tile_owner, start_considered_dt, end_considered_dt ORDER BY final_ranking
        ) = 1
)

SELECT
    row_number() OVER (
        PARTITION BY start_considered_dt, end_considered_dt ORDER BY final_ranking
    ) AS price_winner_ranking,
    final_ranking AS scene_ranking,
    final_score,
    tile_id_base,
    scene_title,
    current_tile_owner,
    start_considered_dt,
    end_considered_dt
FROM duplicate_owners_removed
ORDER BY scene_ranking ASC
LIMIT 30
