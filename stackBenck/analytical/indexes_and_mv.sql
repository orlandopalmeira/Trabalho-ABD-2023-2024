--* Indexes
SELECT 
    idx.indexname AS index_name,
    idx.tablename AS table_name
FROM 
    pg_indexes idx
LEFT JOIN 
    pg_constraint con ON idx.indexname = con.conname
WHERE 
    idx.schemaname = 'public' -- Change to your schema if needed
    AND con.contype IS NULL
ORDER BY 2;

--* Materialized Views
SELECT 
    relname AS materialized_view_name, 
    relnamespace::regnamespace AS schema_name
FROM 
    pg_class 
WHERE 
    relkind = 'm';

--* Trigger Functions
SELECT tgname AS trigger_name,
       tgrelid::regclass AS table_name,
       tgdeferrable AS deferrable,
       tginitdeferred AS init_deferred,
       tgfoid::regprocedure AS function_name,
       tgtype AS trigger_type
FROM pg_trigger;


-- DROP TRIGGER IF EXISTS update_users_years_trigger ON users;



-- Q1

CREATE INDEX idx_comments_creationdate ON comments (creationdate);
CREATE INDEX idx_questions_creationdate ON questions (creationdate); -- repetido
CREATE INDEX idx_answers_creationdate ON answers (creationdate);


-- Q2

CREATE INDEX idx_votes_creationdate ON votes (creationdate);
CREATE INDEX idx_users_creationdate_year on users (extract(year from creationdate));



-- Q3
CREATE INDEX idx_answers_parentid ON answers (parentid);
-- CREATE MATERIALIZED VIEW mv_tags AS
-- SELECT id, tagname
-- FROM tags;



-- Q4

CREATE MATERIALIZED VIEW badges_mat_view AS
SELECT date
FROM badges
WHERE NOT tagbased
    AND name NOT IN (
        'Analytical',
        'Census',
        'Documentation Beta',
        'Documentation Pioneer',
        'Documentation User',
        'Reversal',
        'Tumbleweed'
    )
    AND class in (1, 2, 3)
    AND userid <> -1;


-- CREATE OR REPLACE FUNCTION refresh_badges_mat_view()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     REFRESH MATERIALIZED VIEW badges_mat_view;
--     RETURN NULL;
-- END;
-- $$ LANGUAGE plpgsql;


-- CREATE TRIGGER trigger_refresh_badges_mat_view
-- AFTER INSERT OR DELETE ON badges
-- FOR EACH STATEMENT
-- EXECUTE FUNCTION refresh_badges_mat_view();

-- DROP TRIGGER trigger_refresh_badges_mat_view ON badges;
-- DROP FUNCTION refresh_badges_mat_view();
-- DROP MATERIALIZED VIEW IF EXISTS badges_mat_view CASCADE;
