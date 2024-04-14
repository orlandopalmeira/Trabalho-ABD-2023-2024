select id, title
from questions q
join questionstags qt on qt.questionid = q.id
where qt.tagid = 1
order by q.creationdate desc
limit 25

CREATE INDEX idx_questionstags_tagid ON questionstags (tagid);
CREATE INDEX idx_questions_id ON questions (id); -- Repetido no questionInfo.sql
CREATE INDEX idx_questions_creationdate_desc ON questions (creationdate DESC);


DROP INDEX IF EXISTS idx_questionstags_tagid;
DROP INDEX IF EXISTS idx_questions_id;
DROP INDEX IF EXISTS idx_questions_creationdate_desc;