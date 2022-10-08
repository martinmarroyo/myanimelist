CREATE MATERIALIZED VIEW anime.anime_stats_and_scores
AS
 WITH scores AS (
         SELECT c.anime_id,
            c.load_date,
            c."1",
            c."2",
            c."3",
            c."4",
            c."5",
            c."6",
            c."7",
            c."8",
            c."9",
            c."10"
           FROM crosstab('
			SELECT 
				anime_id
				,load_date::DATE
				,score
				,votes
			FROM
				anime.anime_scores
			ORDER BY
				anime_id
				,load_date
				,score
		'::text, '
			SELECT DISTINCT score
			FROM anime.anime_scores
		'::text) c(anime_id bigint, load_date date, "1" integer, "2" integer, "3" integer, "4" integer, "5" integer, "6" integer, "7" integer, "8" integer, "9" integer, "10" integer)
        )
 SELECT scores.anime_id,
    anime.title AS anime_title,
    stats.watching,
    stats.completed,
    stats.on_hold,
    stats.dropped,
    stats.plan_to_watch,
    stats.total,
    scores."1",
    scores."2",
    scores."3",
    scores."4",
    scores."5",
    scores."6",
    scores."7",
    scores."8",
    scores."9",
    scores."10",
    scores.load_date
   FROM scores
     JOIN ( SELECT all_anime.id,
            all_anime.title
           FROM anime.all_anime) anime ON scores.anime_id = anime.id
     JOIN anime.anime_stats stats ON scores.anime_id = stats.anime_id AND scores.load_date = stats.load_date::date
WITH DATA;


CREATE INDEX idx_anime_stats
    ON anime.anime_stats_and_scores USING btree
    (anime_id, load_date)
    TABLESPACE pg_default;
CREATE INDEX idx_anime_stats_titles
    ON anime.anime_stats_and_scores USING btree
    (anime_title COLLATE pg_catalog."default")
    TABLESPACE pg_default;