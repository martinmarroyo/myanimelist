CREATE TABLE IF NOT EXISTS anime.anime_stats
(
    anime_id bigint NOT NULL,
    watching integer NOT NULL,
    completed integer NOT NULL,
    on_hold integer NOT NULL,
    dropped integer NOT NULL,
    plan_to_watch integer NOT NULL,
    total integer NOT NULL,
    load_date timestamp without time zone,
    CONSTRAINT anime_stats_pkey PRIMARY KEY (anime_id, watching, completed, on_hold, dropped, plan_to_watch, total)
);

TABLESPACE pg_default;


COMMENT ON COLUMN anime.anime_stats.anime_id
    IS 'The id of the anime';

COMMENT ON COLUMN anime.anime_stats.watching
    IS 'Number of users watching the resource';

COMMENT ON COLUMN anime.anime_stats.completed
    IS 'Number of users who have completed the resource';

COMMENT ON COLUMN anime.anime_stats.on_hold
    IS 'Number of users who have put the resource on hold';

COMMENT ON COLUMN anime.anime_stats.dropped
    IS 'Number of users who have dropped the resource';

COMMENT ON COLUMN anime.anime_stats.plan_to_watch
    IS 'Number of users who have planned to watch the resource';

COMMENT ON COLUMN anime.anime_stats.total
    IS 'Total number of users who have the resource added to their lists';

COMMENT ON COLUMN anime.anime_stats.load_date
    IS 'Date/time that stats were extracted and loaded';