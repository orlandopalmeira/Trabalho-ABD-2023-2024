WITH FilteredTags AS (
    SELECT tagid
    FROM TagQuestionCounts
    GROUP BY tagid
    HAVING COUNT(*) > 10
)

SELECT t.tagname, ROUND(AVG(tqc.total), 3) AS avg_total, COUNT(*) AS tag_count
FROM TagQuestionCounts tqc
JOIN FilteredTags ft ON ft.tagid = tqc.tagid
LEFT JOIN tags t ON t.id = tqc.tagid
GROUP BY t.tagname
ORDER BY avg_total DESC, tag_count DESC, t.tagname;


-- Com materialized view

-- CREATE MATERIALIZED VIEW TagQuestionCounts AS
--     SELECT qt.tagid, qt.questionid, COUNT(*) AS total
--     FROM questionstags qt
--     LEFT JOIN answers a ON a.parentid = qt.questionid
--     GROUP BY qt.tagid, qt.questionid


-- 4.14 segundos