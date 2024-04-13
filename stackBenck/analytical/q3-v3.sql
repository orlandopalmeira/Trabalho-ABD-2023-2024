SELECT tagname, ROUND(AVG(total), 3), COUNT(*)
FROM (
    SELECT t.tagname, qt.questionid, COUNT(a.parentid) AS total
    FROM tags t
    JOIN questionstags qt ON qt.tagid = t.id
    LEFT JOIN answers a ON a.parentid = qt.questionid
    WHERE t.id IN (
        SELECT tagid
        FROM questionstags
        GROUP BY tagid
        HAVING COUNT(*) > 10
    )
    GROUP BY t.tagname, qt.questionid
) AS subquery
GROUP BY tagname
ORDER BY 2 DESC, 3 DESC, tagname;