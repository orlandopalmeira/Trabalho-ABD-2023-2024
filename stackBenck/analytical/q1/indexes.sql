-- Active: 1712845059097@@127.0.0.1@5432@stack
CREATE INDEX idx_comments_creationdate ON comments (creationdate);
CREATE INDEX idx_questions_creationdate ON questions (creationdate);
CREATE INDEX idx_answers_creationdate ON answers (creationdate);

-- DROP
-- DROP INDEX idx_comments_creationdate;
-- DROP INDEX idx_questions_creationdate;
-- DROP INDEX idx_answers_creationdate;