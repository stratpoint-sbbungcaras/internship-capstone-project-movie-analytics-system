CREATE OR REPLACE VIEW capstone.company_yearly_profit_rank AS
    SELECT
        c.company_name AS film_studio,
        d.year AS year,
        SUM(f.budget) AS total_budget,
        SUM(f.revenue) AS total_revenue,
        SUM((f.revenue - f.budget) / mc.num_companies) AS total_profit,   -- profit divided
        RANK() OVER (PARTITION BY d.year ORDER BY SUM((f.revenue - f.budget) / mc.num_companies) DESC) AS profit_rank_in_year
    FROM
        capstone.fact_movies f
    JOIN
        capstone.bridge_companies bc ON f.movie_id = bc.movie_id
    JOIN
        capstone.dim_companies c ON bc.company_id = c.company_id
    JOIN
        capstone.dim_date d ON f.release_date_id = d.date_id
    JOIN
        (
        SELECT movie_id, COUNT(*) AS num_companies
        FROM capstone.bridge_companies
        GROUP BY movie_id) mc ON f.movie_id = mc.movie_id
    GROUP BY
        c.company_name, d.year;
