#!/usr/bin/env python3
"""
etl.py
Builds an ETL pipeline for MovieLens + OMDb.
Usage:
    python etl.py --movies movies.csv --ratings ratings.csv --omdb-key YOUR_KEY --db sqlite:///movies.db
"""

import argparse, json, os, time, requests, pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Text, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

CACHE_FILE = "omdb_cache.json"
REQUEST_DELAY = 0.2  # seconds between OMDb requests

# ---------- DB SETUP ----------
def get_engine(db_url: str):
    return create_engine(db_url, echo=False, future=True)

def create_tables(engine):
    meta = MetaData()
    movies = Table('movies', meta,
                   Column('movie_id', Integer, primary_key=True, autoincrement=False),
                   Column('title', String, nullable=False),
                   Column('imdb_id', String, unique=True),
                   Column('year', Integer),
                   Column('plot', Text),
                   Column('director', String),
                   Column('box_office', String))
    genres = Table('genres', meta,
                   Column('genre_id', Integer, primary_key=True, autoincrement=True),
                   Column('name', String, unique=True))
    movie_genres = Table('movie_genres', meta,
                         Column('movie_id', Integer, ForeignKey('movies.movie_id')),
                         Column('genre_id', Integer, ForeignKey('genres.genre_id')),
                         UniqueConstraint('movie_id', 'genre_id'))
    ratings = Table('ratings', meta,
                    Column('rating_id', Integer, primary_key=True, autoincrement=True),
                    Column('user_id', Integer),
                    Column('movie_id', Integer, ForeignKey('movies.movie_id')),
                    Column('rating', Float),
                    Column('timestamp', Integer))
    meta.create_all(engine)
    return {'movies': movies, 'genres': genres, 'movie_genres': movie_genres, 'ratings': ratings}

# ---------- CACHE ----------
def load_cache():
    return json.load(open(CACHE_FILE)) if os.path.exists(CACHE_FILE) else {}

def save_cache(cache):
    with open(CACHE_FILE, "w") as f: json.dump(cache, f, indent=2)

# ---------- HELPERS ----------
def parse_title_and_year(raw):
    if raw.endswith(")") and "(" in raw:
        try:
            y = raw[raw.rfind("(")+1:-1]
            year = int(y) if y.isdigit() else None
            return raw[:raw.rfind("(")].strip(), year
        except Exception:
            pass
    return raw, None

def split_genres(g):
    if not g or g == "(no genres listed)":
        return []
    return [x.strip() for x in g.split("|") if x.strip()]

def query_omdb(title, year, key, cache):
    cache_key = f"{title}||{year}"
    if cache_key in cache:
        return cache[cache_key]

    params = {"t": title, "apikey": key}
    if year:
        params["y"] = str(year)

    try:
        resp = requests.get("http://www.omdbapi.com/", params=params, timeout=15)
        time.sleep(0.3)  # slow down to be kind to free API
        if resp.status_code == 200:
            data = resp.json()
            cache[cache_key] = data
            save_cache(cache)
            return data
        else:
            print(f"⚠️ OMDb HTTP Error {resp.status_code} for: {title}")
            cache[cache_key] = {"Response": "False", "Error": f"HTTP {resp.status_code}"}
            save_cache(cache)
            return cache[cache_key]

    except requests.exceptions.ReadTimeout:
        print(f"⏳ Timeout fetching: {title}. Skipping...")
        cache[cache_key] = {"Response": "False", "Error": "Timeout"}
        save_cache(cache)
        return cache[cache_key]

    except Exception as e:
        print(f"❌ Error fetching OMDb for '{title}': {e}")
        cache[cache_key] = {"Response": "False", "Error": str(e)}
        save_cache(cache)
        return cache[cache_key]


# ---------- ETL ----------
def extract(movies_path, ratings_path):
    return pd.read_csv(movies_path), pd.read_csv(ratings_path)

def transform_and_enrich(movies_df, ratings_df, api_key):
    cache = load_cache()
    movies_out, genres, map_genres = [], set(), {}
    for _, row in movies_df.iterrows():
        movie_id = int(row['movieId'])
        title, year = parse_title_and_year(row['title'])
        glist = split_genres(row['genres'])
        omdb = query_omdb(title, year, api_key, cache) if api_key else {}
        imdb_id = omdb.get('imdbID') if omdb.get('Response') == 'True' else None
        movies_out.append({
            'movie_id': movie_id,
            'title': title,
            'imdb_id': imdb_id,
            'year': year,
            'plot': omdb.get('Plot') if omdb.get('Response') == 'True' else None,
            'director': omdb.get('Director') if omdb.get('Response') == 'True' else None,
            'box_office': omdb.get('BoxOffice') if omdb.get('Response') == 'True' else None,
            'genres': glist
        })
        for g in glist: genres.add(g)
        map_genres[movie_id] = glist
    ratings_df['rating'] = pd.to_numeric(ratings_df['rating'], errors='coerce')
    ratings_df = ratings_df.dropna(subset=['rating'])
    return movies_out, list(genres), map_genres, ratings_df.to_dict(orient='records')

def load_data(engine, tables, movies_out, genres, map_genres, ratings):
    conn = engine.connect(); trans = conn.begin()
    try:
        # Genres
        g_tbl = tables['genres']
        for g in genres:
            conn.execute(sqlite_insert(g_tbl).values(name=g).prefix_with("OR IGNORE"))
        # gid_map = {r['name']: r['genre_id'] for r in conn.execute(g_tbl.select())}
        res = conn.execute(g_tbl.select())
        gid_map = {row._mapping['name']: row._mapping['genre_id'] for row in res}
        # Movies
        m_tbl = tables['movies']
        for m in movies_out:
            conn.execute(sqlite_insert(m_tbl).values(**{k:v for k,v in m.items() if k!='genres'}).prefix_with("OR REPLACE"))
        # Movie-Genres
        mg_tbl = tables['movie_genres']
        for mid, glist in map_genres.items():
            for g in glist:
                if g in gid_map:
                    conn.execute(sqlite_insert(mg_tbl).values(movie_id=mid, genre_id=gid_map[g]).prefix_with("OR IGNORE"))
        # Ratings
        r_tbl = tables['ratings']
        for r in ratings:
            conn.execute(r_tbl.insert().values(user_id=int(r['userId']), movie_id=int(r['movieId']),
                                               rating=float(r['rating']),
                                               timestamp=int(r['timestamp']) if 'timestamp' in r else None))
        trans.commit()
    except Exception as e:
        trans.rollback(); raise
    finally:
        conn.close()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--movies", required=True)
    p.add_argument("--ratings", required=True)
    p.add_argument("--db", default="sqlite:///movies.db")
    p.add_argument("--omdb-key", default=None)
    a = p.parse_args()

    movies_df, ratings_df = extract(a.movies, a.ratings)
    engine = get_engine(a.db)
    tables = create_tables(engine)
    m_out, g_list, m_map, r_out = transform_and_enrich(movies_df, ratings_df, a.omdb_key)
    load_data(engine, tables, m_out, g_list, m_map, r_out)
    print("✅ ETL completed successfully!")

if __name__ == "__main__":
    main()
