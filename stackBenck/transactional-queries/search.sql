select id, title
from questions
where to_tsvector('english', title) @@ to_tsquery('english', 'java')
limit 25


CREATE INDEX idx_questions_title_gin ON questions USING GIN(to_tsvector('english', title));

DROP INDEX IF EXISTS idx_questions_title_gin;
