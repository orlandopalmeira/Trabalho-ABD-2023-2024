SELECT t.tagname,
       round(sum(total) / count(*), 3) AS round,
       count(*) AS count
FROM (
    SELECT qt.tagid, qt.questionid, count(*) AS total
    FROM questionstags qt
    LEFT JOIN answers a ON a.parentid = qt.questionid
    WHERE qt.tagid IN (
        SELECT tagid
        FROM questionstags
        GROUP BY tagid
        HAVING count(*) > 10
    )
    GROUP BY qt.tagid, qt.questionid
) AS subquery
LEFT JOIN tags t ON t.id = subquery.tagid
GROUP BY t.tagname
ORDER BY round DESC, count DESC, tagname;

