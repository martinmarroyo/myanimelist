CREATE OR REPLACE FUNCTION anime_stage.insert_anime_stats(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
/*
    Unfolds raw JSON and stores
    anime statistics in
    anime_stats table
*/
INSERT INTO anime.anime_stats (
	SELECT 
		anime_id
		,(scores -> 'watching')::INT
		AS watching
		,(scores -> 'completed')::INT
		AS completed
		,(scores -> 'on_hold')::INT
		AS on_hold
		,(scores -> 'dropped')::INT
		AS dropped
		,(scores -> 'plan_to_watch')::INT
		AS plan_to_watch
		,(scores -> 'total')::INT
		AS total
		,load_date
	FROM
		anime_stage.anime_stats_scores
)
ON CONFLICT DO NOTHING;
$BODY$;