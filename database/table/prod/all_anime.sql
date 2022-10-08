CREATE TABLE IF NOT EXISTS anime.all_anime
(
    id bigint NOT NULL,
    title text COLLATE pg_catalog."default" NOT NULL,
    status text COLLATE pg_catalog."default",
    rating text COLLATE pg_catalog."default",
    score double precision,
    favorites bigint,
    load_date timestamp without time zone,
    airing boolean NOT NULL,
    aired_from date,
    aired_to date,
    CONSTRAINT all_anime_pkey PRIMARY KEY (id, title, airing)
);

COMMENT ON TABLE anime.all_anime
    IS 'All suitable for work anime titles from My Anime List';

COMMENT ON COLUMN anime.all_anime.id
    IS 'The My Anime List id of the anime';

COMMENT ON COLUMN anime.all_anime.title
    IS 'The title of the anime';

COMMENT ON COLUMN anime.all_anime.status
    IS 'Airing status ("Finished Airing" "Currently Airing" "Not yet aired")';

COMMENT ON COLUMN anime.all_anime.rating
    IS 'Anime audience rating ("G - All Ages" "PG - Children" "PG-13 - Teens 13 or older" "R - 17+ (violence & profanity)" "R+ - Mild Nudity" "Rx - Hentai")';

COMMENT ON COLUMN anime.all_anime.score
    IS 'Score';

COMMENT ON COLUMN anime.all_anime.favorites
    IS 'Number of users who have favorited this entry';