WITH year_range AS (
    SELECT generate_series(2008, extract(year from NOW())) AS year
), max_reputation_per_year AS (
    SELECT extract(year FROM creationdate) AS year, max(reputation) AS max_rep
    FROM users
    GROUP BY extract(year FROM creationdate)
), buckets as (
    SELECT yr.year, generate_series(0, GREATEST(mr.max_rep,0), 5000) AS reputation_range
    FROM year_range yr
    LEFT JOIN max_reputation_per_year mr ON yr.year = mr.year
)

SELECT year, reputation_range, count(u.id) total
FROM buckets
LEFT JOIN (
    SELECT DISTINCT u.id, u.creationdate, u.reputation
    FROM users u
    JOIN answers a ON a.owneruserid = u.id
    JOIN votes v ON a.id = v.postid
    JOIN votestypes vt ON vt.id = v.votetypeid
    WHERE vt.name = 'AcceptedByOriginator' AND v.creationdate >= NOW() - INTERVAL '5 year'
) u ON extract(year FROM u.creationdate) = year AND floor(u.reputation / 5000) * 5000 = reputation_range
GROUP BY 1, 2
ORDER BY 1, 2;