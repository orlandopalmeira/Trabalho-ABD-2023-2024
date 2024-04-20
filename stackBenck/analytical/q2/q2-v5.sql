-- CREATE INDEX idx_votes_creationdate ON votes (creationdate);
-- DROP INDEX IF EXISTS idx_votes_creationdate;

-- CREATE INDEX idx_users_creationdate ON users (creationdate);
-- DROP INDEX IF EXISTS idx_users_creationdate;

-- CREATE MATERIALIZED VIEW users_years AS
-- SELECT id, reputation, extract(year FROM creationdate) AS c_year
--    FROM users;
-- DROP MATERIALIZED VIEW IF EXISTS users_years CASCADE;

-- CREATE OR REPLACE FUNCTION update_users_years()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     IF TG_OP = 'INSERT' THEN
--        INSERT INTO users_years (id, reputation, c_year)
--        VALUES (NEW.id, NEW.reputation, extract(year FROM NEW.creationdate));
--    ELSIF TG_OP = 'DELETE' THEN
--        DELETE FROM users_years
--        WHERE id = OLD.id;
--    ELSIF TG_OP = 'UPDATE' THEN
--       UPDATE users_years
--        SET reputation = NEW.reputation, c_year = extract(year FROM NEW.creationdate)
--        WHERE id = NEW.id;
--    END IF;
--    RETURN NULL;
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE TRIGGER update_users_years_trigger
-- AFTER INSERT OR DELETE ON users
-- FOR EACH ROW
-- EXECUTE FUNCTION update_users_years();

-- CREATE INDEX year_idx ON users_years (c_year);
-- DROP INDEX IF EXISTS year_idx;

WITH buckets AS (
    SELECT year,
        generate_series(0, (
            SELECT cast(max(uy.reputation) as int)
            FROM users_years uy
            WHERE uy.c_year = year
        ), 5000) AS reputation_range
    FROM (
        SELECT generate_series(2008, extract(year FROM NOW())) AS year
    ) years
    -- GROUP BY 1, 2
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
) u ON extract(year FROM u.creationdate) = year
    AND floor(u.reputation / 5000) * 5000 = reputation_range
GROUP BY 1, 2
ORDER BY 1, 2;