/* @bruin
name: public.bootstrap_target_table
type: pg.sql
@bruin */

-- Bootstrap initial data into target_table
DROP TABLE IF EXISTS public.target_table;
CREATE TABLE public.target_table AS
SELECT 1 AS pk, 10 AS col_a, 100 AS col_b, 'initial_a' AS col_c, 'default' AS col_d
UNION ALL
SELECT 2 AS pk, 20 AS col_a, 200 AS col_b, 'initial_b' AS col_c, 'default' AS col_d;