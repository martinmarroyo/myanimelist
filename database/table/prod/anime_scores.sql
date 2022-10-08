CREATE TABLE IF NOT EXISTS anime.anime_scores
(
    anime_id bigint NOT NULL,
    score integer NOT NULL,
    votes integer NOT NULL,
    percentage double precision NOT NULL,
    load_date timestamp without time zone,
    CONSTRAINT anime_scores_pkey PRIMARY KEY (anime_id, score, votes, percentage)
);

COMMENT ON COLUMN anime.anime_scores.anime_id
    IS 'The id of the anime';

COMMENT ON COLUMN anime.anime_scores.score
    IS 'Scoring value';

COMMENT ON COLUMN anime.anime_scores.votes
    IS 'Number of votes for this score';

COMMENT ON COLUMN anime.anime_scores.percentage
    IS 'Percentage of votes for this score';

COMMENT ON COLUMN anime.anime_scores.load_date
    IS 'Date/time that scores were extracted and loaded';