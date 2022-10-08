CREATE TABLE IF NOT EXISTS anime_stage.anime_stats_scores
(
    anime_id bigint,
    scores jsonb,
    load_date timestamp without time zone
);