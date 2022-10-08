DROP SCHEMA IF EXISTS anime_stage CASCADE;
DROP SCHEMA IF EXISTS anime CASCADE;

CREATE SCHEMA anime_stage;
CREATE SCHEMA anime;

-- Enable crosstab
CREATE EXTENSION IF NOT EXISTS tablefunc;

COMMENT ON SCHEMA anime
    IS 'This schema holds tables from the Jikan-My Anime List API';