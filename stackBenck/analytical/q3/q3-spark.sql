WITH TagQuestionCounts AS (
    SELECT qt.tagid, qt.questionid, COUNT(*) AS total
    FROM questionstags qt
    LEFT JOIN answers a ON a.parentid = qt.questionid
    GROUP BY qt.tagid, qt.questionid
),
FilteredTags AS (
    SELECT tagid, tagname, count(*) as tag_count
    FROM TagQuestionCounts tqc
    LEFT JOIN tags t ON t.id = tqc.tagid
    GROUP BY tagid, tagname
), mv_q3 as (
    SELECT ft.tagname, ROUND(AVG(tqc.total), 3), ft.tag_count as count
    FROM TagQuestionCounts tqc
    JOIN FilteredTags ft ON ft.tagid = tqc.tagid
    GROUP BY ft.tagname, ft.tag_count
)
SELECT *
FROM mv_q3
WHERE count > 10