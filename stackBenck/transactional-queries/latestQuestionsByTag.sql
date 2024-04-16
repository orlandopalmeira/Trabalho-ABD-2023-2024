select id, title
from questions q
join questionstags qt on qt.questionid = q.id
where qt.tagid = 1
order by q.creationdate desc
limit 25;

-- Modificação na query (remoção do join) (Mesma performance)
WITH tagged_questions AS (
    SELECT id, title, creationdate
    FROM questions
    WHERE id IN (SELECT questionid FROM questionstags WHERE tagid = 1)
)
SELECT id, title
FROM tagged_questions tg
ORDER BY tg.creationdate DESC
LIMIT 25;


CREATE INDEX idx_questionstags_tagid ON questionstags (tagid);
CREATE INDEX idx_questions_creationdate ON questions (creationdate); -- Repetido no q1-improvs.sql


DROP INDEX IF EXISTS idx_questionstags_tagid;
DROP INDEX IF EXISTS idx_questions_creationdate;