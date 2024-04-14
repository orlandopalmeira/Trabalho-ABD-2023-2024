SELECT t.tagname, round(avg(qt.count), 3) AS avg_answers, count(*) AS total_questions
FROM (
    SELECT qt.tagid, qt.questionid, count(a.id) AS count
    FROM questionstags qt
    LEFT JOIN answers a ON a.parentid = qt.questionid
    GROUP BY qt.tagid, qt.questionid
) qt
JOIN tags t ON t.id = qt.tagid
GROUP BY t.tagname
HAVING count(*) > 10
ORDER BY avg_answers DESC, total_questions DESC, t.tagname;