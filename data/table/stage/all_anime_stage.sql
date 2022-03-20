CREATE TABLE IF NOT EXISTS anime_stage.all_anime
(
    id bigint,
    title text COLLATE pg_catalog."default",
    status text COLLATE pg_catalog."default",
    rating text COLLATE pg_catalog."default",
    score double precision,
    favorites bigint,
    load_date timestamp without time zone,
    airing boolean,
    aired_from timestamp without time zone,
    aired_to timestamp without time zone
)

TABLESPACE pg_default;

ALTER TABLE anime_stage.all_anime
    OWNER to tmjsqueutymrpo;

-- Trigger: insert_anime

-- DROP TRIGGER insert_anime ON anime_stage.all_anime;

CREATE TRIGGER insert_anime
    AFTER INSERT
    ON anime_stage.all_anime
    FOR EACH STATEMENT
    EXECUTE FUNCTION anime_stage.insert_new_anime();