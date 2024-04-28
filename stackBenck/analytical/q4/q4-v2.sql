SELECT date_bin('1 minute', date, '2008-01-01 00:00:00'), count(*)
FROM badges_mat_view
GROUP BY 1
ORDER BY 1;

-- CREATE MATERIALIZED VIEW badges_mat_view AS
-- SELECT date
-- FROM badges
-- WHERE NOT tagbased
--     AND name NOT IN (
--         'Analytical',
--         'Census',
--         'Documentation Beta',
--         'Documentation Pioneer',
--         'Documentation User',
--         'Reversal',
--         'Tumbleweed'
--     )
--     AND class in (1, 2, 3)
--     AND userid <> -1