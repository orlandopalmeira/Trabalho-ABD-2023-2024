CREATE MATERIALIZED VIEW mv_tags AS
SELECT id, tagname
FROM tags;


SELECT tagname, round(avg(total), 3), count(*)
FROM (
    SELECT t.tagname, qt.questionid, count(*) AS total
    FROM mv_tags t
    JOIN questionstags qt ON qt.tagid = t.id
    LEFT JOIN answers a ON a.parentid = qt.questionid
    WHERE t.id IN (
        SELECT tagid
        FROM questionstags
        GROUP BY tagid
        HAVING count(*) > 10
    )
    GROUP BY t.tagname, qt.questionid
)
GROUP BY tagname
ORDER BY 2 DESC, 3 DESC, tagname;
