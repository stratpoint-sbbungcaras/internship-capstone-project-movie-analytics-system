CREATE OR REPLACE VIEW capstone.company_yearly_profit_sequential AS
    SELECT
        c.company_name AS film_studio,
        d.year AS year,
        SUM(f.budget) AS total_budget,
        SUM(f.revenue) AS total_revenue,
        SUM((f.revenue - f.budget / mc.num_companies)) AS total_profit,
        LAG(SUM(f.revenue - f.budget), 1, 0) OVER (PARTITION BY c.company_name ORDER BY d.year) AS previous_year_profit
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
