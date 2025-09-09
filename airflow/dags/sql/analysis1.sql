CREATE OR REPLACE VIEW capstone.total_companies_in_movie AS
    SELECT movie_id, COUNT(*) AS num_companies
    FROM capstone.bridge_companies
    GROUP BY movie_id;
