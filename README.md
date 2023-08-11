# DAO Scenes Ranking

Queries to reproduce the Scenes Ranking.

First, you need to have available these raw datasets as a table:
* rel_scenes_tiles (Link AWS)
* scene_wallet_movement (Link AWS)
* scene_ranking_weights (Link AWS)

The following queries should be executed on the order below:

1. scene_wallet_visit_duration.sql -> All visits and stay durations to each scene by user that are then
2. scene_visit_duration.sql -> Metrics with the aggregated user activities for each scene
3. top_scenes_monthly.sql -> We keep the top 100 scenes in terms of visits
4. scene_ranking_monthly.sql -> The top 100 scenes are ranked according to visists, unique visitors, length of scene stays, retention and returning visists from the same users. The rankings for each metric are taken into account to create a final ranking.
5. scene_ranking_winners_monthly.sql -> The final winners list, these are going to be the 30 tile owners that had the best scenes, with only the best scene of each owner taken into consideration.

These queries have place holders in the `FROM` statements, you should manually replace them with references to the actual tables on your local db.