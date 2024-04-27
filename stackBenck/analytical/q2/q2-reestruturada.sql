-- Active: 1708441282596@@127.0.0.1@5432@stack
-- ||||||||||||||| Versão original |||||||||||||||

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
    GROUP BY 1, 2
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

-- ||||||||||||||| Primeira alteração |||||||||||||||

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
    -- GROUP BY 1, 2 -- O group by sai, pois não é necessário
)
SELECT year, reputation_range, count(u.id) total
FROM buckets
LEFT JOIN (
    SELECT id, creationdate, reputation
    FROM users
    WHERE id in ( -- passa a subquery para join
        SELECT a.owneruserid
        FROM answers a
        JOIN votes v ON a.id = v.postid
        JOIN votestypes vt ON vt.id = v.votetypeid
        WHERE vt.name = 'AcceptedByOriginator' AND v.creationdate >= NOW() - INTERVAL '5 year'
    )
) u ON extract(year FROM u.creationdate) = year
    AND floor(u.reputation / 5000) * 5000 = reputation_range
GROUP BY 1, 2
ORDER BY 1, 2;

-- ||||||||||||||| Segunda alteração |||||||||||||||

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
)

SELECT year, reputation_range, count(u.id) total
FROM buckets
LEFT JOIN (
    SELECT DISTINCT u.id, u.creationdate, u.reputation -- tiramos uma subquery e convertemos para join, o DISTINCT é para evitar repetições do u.id
    FROM users u
    JOIN answers a ON a.owneruserid = u.id
    JOIN votes v ON a.id = v.postid
    JOIN votestypes vt ON vt.id = v.votetypeid
    WHERE vt.name = 'AcceptedByOriginator' AND v.creationdate >= NOW() - INTERVAL '5 year'
) u ON extract(year FROM u.creationdate) = year
    AND floor(u.reputation / 5000) * 5000 = reputation_range
GROUP BY 1, 2
ORDER BY 1, 2;

-- ||||||||||||||| Terceira alteração |||||||||||||||
WITH year_range AS (
    SELECT generate_series(2008, extract(year from NOW())) AS year -- anos desde 2008 até 2024
), max_reputation_per_year AS ( -- reputação máxima entre todos os users para cada ano de criação de conta
    SELECT extract(year FROM creationdate) AS year, max(reputation) AS max_rep
    FROM users
    GROUP BY extract(year FROM creationdate)
), buckets as ( -- Alteração ao bucket 
    -- Todos os anos desde 2008 até 2024 e para cada ano, todos os ranges de reputação de 5000 em 5000 até ao máximo de reputação desse ano
    SELECT yr.year, generate_series(0, GREATEST(mr.max_rep,0), 5000) AS reputation_range -- O GREATEST serve para permitir que os anos em que nenhum utilizador se registou apareçam na mesma nos resultados, mas aparecem com a reputation = 0
    FROM year_range yr
    LEFT JOIN max_reputation_per_year mr ON yr.year = mr.year -- O left join é para manter os anos mesmo que não haja registos para eles
)

-- Esta query dá uma distribuição da reputação dos users por cada ano de criação de conta (desde 2008) em intervalos de 5000, mas apenas aqueles que têm respostas "AcceptedByOriginator" nos últimos 5 anos
SELECT year, reputation_range, count(u.id) total
FROM buckets
LEFT JOIN (
    -- Utilizadores que têm respostas "AcceptedByOriginator" nos últimos 5 anos, a sua data de criação de conta e reputação
    SELECT DISTINCT u.id, u.creationdate, u.reputation -- Como só queremos o utilizador em si, usamos o DISTINCT para evitar duplicados
    FROM users u
    JOIN answers a ON a.owneruserid = u.id
    JOIN votes v ON a.id = v.postid
    JOIN votestypes vt ON vt.id = v.votetypeid
    WHERE vt.name = 'AcceptedByOriginator' AND v.creationdate >= NOW() - INTERVAL '5 year'
) u ON extract(year FROM u.creationdate) = year AND floor(u.reputation / 5000) * 5000 = reputation_range
GROUP BY 1, 2
ORDER BY 1, 2;
