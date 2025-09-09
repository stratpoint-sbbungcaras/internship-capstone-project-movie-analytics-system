CREATE OR REPLACE VIEW capstone.genre_yearly_profit_rank AS
    SELECT 
        g.genre_name AS genre,
        d.year AS year,
        SUM(f.budget) AS total_budget,
        SUM(f.revenue) AS total_revenue,
        SUM((f.revenue - f.budget / mc.num_companies)) AS total_profit,
        ROW_NUMBER() OVER (PARTITION BY g.genre_name ORDER BY SUM(f.revenue - f.budget) DESC) AS genre_rank
    FROM capstone.fact_movies f
    JOIN capstone.bridge_genres bg ON f.movie_id = bg.movie_id
    JOIN capstone.dim_genres g ON bg.genre_id = g.genre_id
    JOIN capstone.dim_date d ON f.release_date_id = d.date_id
    JOIN
        (
        SELECT movie_id, COUNT(*) AS num_companies
        FROM capstone.bridge_companies
        GROUP BY movie_id) mc ON f.movie_id = mc.movie_id
    GROUP BY g.genre_name, d.year
    ORDER BY genre, genre_rank;
        
