-- schema.sql
-- Create tables for the movie ETL project (works with SQLite / PostgreSQL / MySQL)

CREATE TABLE IF NOT EXISTS movies (
    movie_id INTEGER PRIMARY KEY,             -- MovieLens movieId
    title TEXT NOT NULL,
    imdb_id TEXT UNIQUE,
    year INTEGER,
    plot TEXT,
    director TEXT,
    box_office TEXT
);

CREATE TABLE IF NOT EXISTS genres (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,
    UNIQUE(movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

CREATE TABLE IF NOT EXISTS ratings (
    rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    rating REAL NOT NULL,
    timestamp INTEGER
);
