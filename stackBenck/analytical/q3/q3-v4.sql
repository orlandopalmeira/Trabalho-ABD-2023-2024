SELECT t.tagname, ROUND(AVG(total), 3) AS average_total, COUNT(*) AS tag_count
FROM (
    SELECT qt.tagid, COUNT(*) AS total
    FROM questionstags qt
    JOIN answers a ON a.parentid = qt.questionid
    GROUP BY qt.tagid, qt.questionid
    HAVING COUNT(*) > 10
) AS tag_counts
JOIN tags t ON t.id = tag_counts.tagid
GROUP BY t.tagname
ORDER BY average_total DESC, tag_count DESC, t.tagname;
