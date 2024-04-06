SELECT u.id, u.displayname,
    count(DISTINCT q.id) + count(DISTINCT a.id) + count(DISTINCT c.id) total
FROM users u
LEFT JOIN questions q ON q.owneruserid = u.id AND q.creationdate BETWEEN now() - interval '6 months' AND now()
LEFT JOIN answers a ON a.owneruserid = u.id AND a.creationdate BETWEEN now() - interval '6 months' AND now()
LEFT JOIN comments c ON c.userid = u.id AND c.creationdate BETWEEN now() - interval '6 months' AND now()
GROUP BY u.id, u.displayname
ORDER BY total DESC
LIMIT 100;

