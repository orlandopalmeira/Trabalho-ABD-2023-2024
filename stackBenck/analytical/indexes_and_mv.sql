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
    AND con.contype IS NULL;

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


DROP TRIGGER IF EXISTS update_users_years_trigger ON users;



-- Q1

-- CREATE INDEX idx_comments_creationdate ON comments (creationdate);
-- CREATE INDEX idx_questions_creationdate ON questions (creationdate);
-- CREATE INDEX idx_answers_creationdate ON answers (creationdate);

-- Q2

-- CREATE INDEX idx_votes_creationdate ON votes (creationdate);
-- CREATE INDEX idx_users_creationdate ON users (creationdate);


-- Q3
-- ???
-- CREATE INDEX idx_votes_postid_creationdate ON votes (postid, creationdate);
-- CREATE INDEX idx_questionstags_grouping ON questionstags (tagname, questionid);
-- CREATE INDEX idx_answers_owneruserid ON answers (owneruserid);


-- Q4

-- CREATE MATERIALIZED VIEW badges_mat_view AS
-- SELECT date
-- FROM badges
-- WHERE NOT tagbased
--     AND name NOT IN (
--         'Analytical',
--         'Census',
--         'Documentation Beta',
--         'Documentation Pioneer',
--         'Documentation User',
--         'Reversal',
--         'Tumbleweed'
--     )
--     AND class in (1, 2, 3)
--     AND userid <> -1
