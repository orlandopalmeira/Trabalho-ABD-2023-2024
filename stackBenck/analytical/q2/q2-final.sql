WITH year_range AS (
    SELECT generate_series(2008, extract(year from NOW())) AS year
), max_reputation_per_year AS (
    SELECT extract(year FROM creationdate) AS year, cast(max(reputation) as int) AS max_rep
    FROM users
    GROUP BY extract(year FROM creationdate)
), buckets as (
    SELECT yr.year, generate_series(0, mr.max_rep, 5000) AS reputation_range
    FROM year_range yr
    JOIN max_reputation_per_year mr ON yr.year = mr.year
)

SELECT year, reputation_range, count(u.id) total
FROM buckets
LEFT JOIN (
    SELECT id, creationdate, reputation
    FROM users
    WHERE id in (
        SELECT a.owneruserid
        FROM answers a
        WHERE a.id IN (
            SELECT postid
            FROM votes v
            JOIN votestypes vt ON vt.id = v.votetypeid
            WHERE vt.name = 'AcceptedByOriginator'
                AND v.creationdate >= NOW() - INTERVAL '5 year'
        )
    )
) u ON extract(year FROM u.creationdate) = year AND floor(u.reputation / 5000) * 5000 = reputation_range
GROUP BY 1, 2
ORDER BY 1, 2;