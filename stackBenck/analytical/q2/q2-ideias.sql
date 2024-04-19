-- Active: 1708441413272@@127.0.0.1@5432@stack
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




--> Progresso:

--! Os valores mencionados são locais. É preciso tirar valores da VM para se por em relatorio

--! Talvez ver melhor se isto não é mesmo necessário fazendo uns testes com diferentes argumentos
--* Removi o groupBy da CTE buckets, uma vez que é redundante e segundo o explain analyze, era uma operação com custo muito grande. Melhora de 8.5secs para 6.8 secs


--* Este indice ajuda na subquery mais interna dos votes, uma vez que segundo o EXPLAIN ANALYZE, o filter dos votes demora bastante (2s). Melhora de 6.8 para 5.9
-- CREATE INDEX idx_votes_creationdate ON votes (creationdate);
-- DROP INDEX IF EXISTS idx_votes_creationdate;

--* Este indice ajuda no filtro de users que na CTE buckets, que é uma operação bastante pesada segundo o EXPLAIN ANALYZE (com 4 secs). Melhora de 5.9 para 5.7 --! Para se fazer uma análise mais interessante, fazer-se-ia o explain analyze com os indices
-- CREATE INDEX idx_users_creationdate ON users (creationdate);
-- DROP INDEX IF EXISTS idx_users_creationdate;



--* Fazer uma mat view mas apenas para a extração dos anos dos users e assim ela so tinha de ser atualizada com INSERTS OU DELETES
--* Melhora o tempo de 5.7 para 3.7
-- CREATE MATERIALIZED VIEW users_years AS
-- SELECT id, reputation, extract(year FROM creationdate) AS c_year
--     FROM users;
-- DROP MATERIALIZED VIEW IF EXISTS users_years CASCADE;

--* Melhora o tempo de 3.7 para 2.2
-- CREATE INDEX year_idx ON users_years (c_year);
-- DROP INDEX IF EXISTS year_idx;

--* Trigger para atualizar a mat view users_years
--! NAO TESTADO
-- CREATE OR REPLACE FUNCTION update_users_years()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     IF TG_OP = 'INSERT' THEN
--         INSERT INTO users_years (id, reputation, c_year)
--         VALUES (NEW.id, NEW.reputation, extract(year FROM NEW.creationdate));
--     ELSIF TG_OP = 'DELETE' THEN
--         DELETE FROM users_years
--         WHERE id = OLD.id;
--     ELSIF TG_OP = 'UPDATE' THEN
--         UPDATE users_years
--         SET reputation = NEW.reputation, c_year = extract(year FROM NEW.creationdate)
--         WHERE id = NEW.id;
--     END IF;
--     RETURN NULL;
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE TRIGGER update_users_years_trigger
-- AFTER INSERT OR DELETE ON users
-- FOR EACH ROW
-- EXECUTE FUNCTION update_users_years();
