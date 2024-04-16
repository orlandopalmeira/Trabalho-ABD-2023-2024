SELECT t.tagname, round(avg(total), 3), count(*)
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
)
LEFT JOIN tags t ON t.id = tagid
GROUP BY t.tagname
ORDER BY 2 DESC, 3 DESC, tagname;