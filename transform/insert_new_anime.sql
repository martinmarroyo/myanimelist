CREATE OR REPLACE FUNCTION anime_stage.insert_new_anime()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
/*
    Inserts new anime entries
    from API pull into all_anime
    table.
*/
INSERT INTO anime.all_anime(
	SELECT
		id
		,title
		,status
		,rating
		,score
		,favorites
		,load_date
		,airing
		,aired_from::DATE
		,aired_to::DATE
	FROM anime_stage.all_anime
)
ON CONFLICT DO NOTHING;

RETURN NULL;

END;
$BODY$;