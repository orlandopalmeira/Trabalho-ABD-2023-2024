# Índices para a query 1
CREATE INDEX idx_comments_creationdate ON comments (creationdate);
CREATE INDEX idx_questions_creationdate ON questions (creationdate);
CREATE INDEX idx_answers_creationdate ON answers (creationdate);

DROP INDEX idx_comments_creationdate;
DROP INDEX idx_questions_creationdate;
DROP INDEX idx_answers_creationdate;

# Query1 simplificada 
-- SELECT u.id, u.displayname,
--     count(DISTINCT q.id) + count(DISTINCT a.id) + count(DISTINCT c.id) total
-- FROM users u
-- LEFT JOIN questions q ON q.owneruserid = u.id AND q.creationdate BETWEEN now() - interval '6 months' AND now()
-- LEFT JOIN answers a ON a.owneruserid = u.id AND a.creationdate BETWEEN now() - interval '6 months' AND now()
-- LEFT JOIN comments c ON c.userid = u.id AND c.creationdate BETWEEN now() - interval '6 months' AND now()
-- GROUP BY u.id, u.displayname --! Aqui talvez se pudesse remover o u.displayname, mas não se nota nenhuma diferença de tempo
-- ORDER BY total DESC
-- LIMIT 100;
