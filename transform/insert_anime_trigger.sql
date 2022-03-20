CREATE TRIGGER insert_anime
    AFTER INSERT
    ON anime_stage.all_anime
    FOR EACH STATEMENT
    EXECUTE FUNCTION anime_stage.insert_new_anime();