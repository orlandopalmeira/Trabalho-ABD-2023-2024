WITH buckets AS (
    SELECT year,
        generate_series(0, (
            SELECT cast(max(reputation) as int)
            FROM users
            WHERE extract(year FROM creationdate) = year
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


--! Os valores mencionados são locais. É preciso tirar valores da VM para se por em relatorio

--! Talvez ver melhor se isto não é mesmo necessário fazendo uns testes com diferentes argumentos
--* Removi o groupBy da CTE buckets, uma vez que é redundante e segundo o explain analyze, era uma operação com custo muito grande. Melhora de 8.5secs para 6.8 secs


--* Este indice ajuda na subquery mais interna dos votes, uma vez que segundo o EXPLAIN ANALYZE, o filter dos votes demora bastante (2s). Melhora de 6.8 para 5.9
-- CREATE INDEX idx_votes_creationdate ON votes (creationdate);
-- DROP INDEX IF EXISTS idx_votes_creationdate;

--* Este indice ajuda no filtro de users que na CTE buckets, que é uma operação bastante pesada segundo o EXPLAIN ANALYZE (com 4 secs). Melhora de 5.9 para 5.7
-- CREATE INDEX idx_users_creationdate ON users (creationdate);
-- DROP INDEX IF EXISTS idx_users_creationdate;



--* Pensei nesta mat view mas depois vi que ela é muito insignificante, uma vez que é muito rápida
-- CREATE MATERIALIZED VIEW vote_types_acceptedbyoriginator AS
-- SELECT *
-- FROM votestypes
-- WHERE name = 'AcceptedByOriginator';