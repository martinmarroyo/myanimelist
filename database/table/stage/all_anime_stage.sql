CREATE TABLE IF NOT EXISTS anime_stage.all_anime
(
    id bigint,
    title text COLLATE pg_catalog."default",
    status text COLLATE pg_catalog."default",
    rating text COLLATE pg_catalog."default",
    score float,
    favorites int,
    load_date timestamp without time zone,
    airing boolean,
    aired_from text,
    aired_to text
);