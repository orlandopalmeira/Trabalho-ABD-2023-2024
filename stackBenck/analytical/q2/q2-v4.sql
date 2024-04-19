--CREATE MATERIALIZED VIEW buckets_mv AS
--SELECT year,
--       generate_series(0, (
--            SELECT cast(max(reputation) as int)
--            FROM users
--            WHERE extract(year FROM creationdate) = year
--        ), 5000) AS reputation_range
--FROM (
--    SELECT generate_series(2008, extract(year FROM NOW())) AS year
--) years
--GROUP BY 1, 2;


SELECT year, reputation_range, count(u.id) total
FROM buckets_mv
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
) u ON extract(year FROM u.creationdate) = year
    AND floor(u.reputation / 5000) * 5000 = reputation_range
GROUP BY 1, 2
ORDER BY 1, 2;


--CREATE OR REPLACE FUNCTION update_buckets_mv()
--RETURNS TRIGGER AS $$
--BEGIN
--    REFRESH MATERIALIZED VIEW buckets_mv;
--    RETURN NULL;
--END;
--$$ LANGUAGE plpgsql;

--CREATE TRIGGER trigger_update_buckets_mv
--AFTER INSERT OR DELETE OR UPDATE ON users
--FOR EACH STATEMENT
--EXECUTE FUNCTION update_buckets_mv();

--DROP TRIGGER IF EXISTS trigger_update_buckets_mv ON users;
--DROP FUNCTION IF EXISTS update_buckets_mv();

--DROP MATERIALIZED VIEW IF EXISTS buckets_mv;
