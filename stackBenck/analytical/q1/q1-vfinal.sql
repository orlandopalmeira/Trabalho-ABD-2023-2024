WITH interactions AS (
    SELECT
        owneruserid,
        COUNT(*) AS interaction_count
    FROM (
        SELECT owneruserid, creationdate FROM questions
        UNION ALL
        SELECT owneruserid, creationdate FROM answers
        UNION ALL
        SELECT userid AS owneruserid, creationdate FROM comments
    ) AS all_interactions
    WHERE creationdate BETWEEN NOW() - INTERVAL '6 months' AND NOW()
    GROUP BY owneruserid
)

SELECT
    u.id,
    u.displayname,
    COALESCE(q.interaction_count, 0) AS total
FROM
    users u
LEFT JOIN interactions q ON q.owneruserid = u.id
ORDER BY total DESC
LIMIT 100;

-- CREATE INDEX idx_comments_creationdate ON comments (creationdate);
-- CREATE INDEX idx_questions_creationdate ON questions (creationdate);
-- CREATE INDEX idx_answers_creationdate ON answers (creationdate);