-- queries.sql

-- 1. Movie with the highest average rating
SELECT m.title, AVG(r.rating) AS avg_rating, COUNT(*) AS num_ratings
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title
ORDER BY avg_rating DESC
LIMIT 1;

-- 2. Top 5 genres with highest average rating
SELECT g.name AS genre, AVG(r.rating) AS avg_rating
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN ratings r ON mg.movie_id = r.movie_id
GROUP BY g.genre_id, g.name
ORDER BY avg_rating DESC
LIMIT 5;

-- 3. Director with most movies
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL AND director <> ''
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- 4. Average rating per release year
SELECT m.year, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
WHERE m.year IS NOT NULL
GROUP BY m.year
ORDER BY m.year;
