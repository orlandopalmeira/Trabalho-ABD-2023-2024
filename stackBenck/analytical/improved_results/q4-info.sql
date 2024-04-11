-- Active: 1712845059097@@127.0.0.1@5432@stack
-- # Índices para a query 4
-- # pensou-se em criar um indice para a data, mas verificamos que não é utilizado o indice, uma vez que o agrupamento é feito em relação ao resultado de date_bin e não da data em si (por verificar com EXPLAIN ANALYZE)

CREATE INDEX idx_badges_date ON badges(date);
CREATE INDEX idx_badges_name_class_userid ON badges(tagbased, userid, class, name);


DROP INDEX idx_badges_date;
DROP INDEX idx_badges_name_class_userid;
