-- # pensou-se em criar um indice para a data, mas verificamos que não é utilizado o indice, uma vez que o agrupamento é feito em relação ao resultado de date_bin e não da data em si (por verificar com EXPLAIN ANALYZE)
-- # um indice nos nomes também não se provou muito útil, mas não sei bem porquê (talvez porque o filtro é feito em relação a uma lista de valores)(verificar melhor a veracidade disto)
-- # um indice no campo tagbased também não se provou muito útil 
-- # também tentamos criar um indice composto com os campos tagbased e name, mas também não foi aproveitado pelo planner

-- Indices b-tree (nao deram resultado)
CREATE INDEX idx_badges_date ON badges_mat_view (date);
DROP INDEX IF EXISTS idx_badges_date;

-- Indices de hash demoram muito tempo a criar devido a colisoes, não tendo sido uma estratégia a seguir.
CREATE INDEX idx_badges_name ON badges USING hash (name);



CREATE MATERIALIZED VIEW badges_mat_view AS
SELECT date
FROM badges
WHERE NOT tagbased
    AND name NOT IN (
        'Analytical',
        'Census',
        'Documentation Beta',
        'Documentation Pioneer',
        'Documentation User',
        'Reversal',
        'Tumbleweed'
    )
    AND class in (1, 2, 3)
    AND userid <> -1;

CREATE OR REPLACE FUNCTION refresh_badges_mat_view()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW badges_mat_view;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_refresh_badges_mat_view
AFTER INSERT OR DELETE ON badges
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_badges_mat_view();

-- DROP all the that was created before
DROP MATERIALIZED VIEW IF EXISTS badges_mat_view CASCADE;
DROP TRIGGER trigger_refresh_badges_mat_view ON badges;
DROP FUNCTION refresh_badges_mat_view();

-- List all triggers
SELECT tgname AS trigger_name,
       tgrelid::regclass AS table_name,
       tgfoid::regprocedure AS trigger_function,
       tgenabled AS trigger_status
FROM pg_trigger;

