WITH questionstags_tagid as 
    (SELECT tagid
    FROM questionstags
    GROUP BY tagid
    HAVING count(*) > 10)
SELECT t.tagname, round(avg(total), 3), count(*)
FROM (
    SELECT qt.tagid, qt.questionid, count(*) AS total
    FROM questionstags qt
    inner join questionstags_tagid qtt on qtt.tagid = qt.tagid
    LEFT JOIN answers a ON a.parentid = qt.questionid
    GROUP BY qt.tagid, qt.questionid
)
LEFT JOIN tags t ON t.id = tagid
GROUP BY t.tagname
ORDER BY 2 DESC, 3 DESC, tagname;