SELECT u.id, u.displayname, (COALESCE(qcount, 0)+COALESCE(acount, 0)+COALESCE(ccount, 0)) AS total
FROM users u
LEFT JOIN (
    SELECT owneruserid, count(*) AS qcount
    FROM questions WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    GROUP BY owneruserid
) q ON q.owneruserid = u.id
LEFT JOIN (
    SELECT owneruserid, count(*) AS acount
    FROM answers WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    GROUP BY owneruserid
) a ON a.owneruserid = u.id
LEFT JOIN (
    SELECT userid, count(*) AS ccount
    FROM comments WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    GROUP BY userid
) c ON c.userid = u.id
ORDER BY total DESC
LIMIT 100;

-- CREATE INDEX idx_comments_creationdate ON comments (creationdate);
-- CREATE INDEX idx_questions_creationdate ON questions (creationdate);
-- CREATE INDEX idx_answers_creationdate ON answers (creationdate);