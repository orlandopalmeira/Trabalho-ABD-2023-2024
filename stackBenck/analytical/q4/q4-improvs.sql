-- # pensou-se em criar um indice para a data, mas verificamos que não é utilizado o indice, uma vez que o agrupamento é feito em relação à nova coluna criada pela função date_bin() e não relativamente à coluna data.
-- # um indice nos nomes também não se provou muito útil, mas não sei bem porquê (talvez porque o filtro é feito em relação a uma lista de valores)(verificar melhor a veracidade disto)
-- # um indice no campo tagbased também não se provou muito útil 
-- # também tentamos criar um indice composto com os campos tagbased e name, mas também não foi aproveitado pelo planner



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

