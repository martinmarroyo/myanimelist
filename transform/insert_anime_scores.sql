CREATE OR REPLACE FUNCTION anime_stage.insert_anime_scores(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
/*
    Unfolds raw anime scores
    and uploads data into
    anime_scores table
*/
INSERT INTO anime.anime_scores (
	WITH scores AS (
		SELECT
			anime_id
			,jsonb_array_elements(scores->'scores')
			AS scores
			,load_date
		FROM
			anime_stage.anime_stats_scores
	)
	SELECT
		anime_id
		,(scores->'score')::INT
		AS score
		,(scores->'votes')::INT
		AS votes
		,(scores->'percentage')::DOUBLE PRECISION
		AS percentage
		,load_date
	FROM scores
)
ON CONFLICT DO NOTHING;
$BODY$;