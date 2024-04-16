select title, body, creationdate, owneruserid, acceptedanswerid, tag_list, answers_list, links_list
    from questions q
    -- tags
    left join (
        select qt.questionid, array_agg(tagname) as tag_list
        from tags t
        join questionstags qt on qt.tagid = t.id
        group by qt.questionid
    ) t on t.questionid = q.id
    -- answers
    left join (
        select a.parentid, json_agg(json_build_object('user', a.owneruserid, 'body', a.body)) as answers_list
        from answers a
        group by a.parentid
    ) a on a.parentid = q.id
    -- links
    left join (
        select ql.questionid, json_agg(json_build_object('question', ql.relatedquestionid, 'type', ql.linktypeid)) as links_list
        from questionslinks ql
        group by ql.questionid
    ) ql on ql.questionid = q.id
    where q.id = 31127474;


CREATE INDEX idx_answers_parentid ON answers (parentid);

DROP INDEX IF EXISTS idx_answers_parentid;

