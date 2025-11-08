import sqlite3
import re
from textwrap import indent

def run_queries(db_path="movies.db", sql_file="queries.sql"):
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("\n🎬  Connected to database successfully:", db_path)

        # Read SQL file
        with open(sql_file, "r", encoding="utf-8") as f:
            sql_content = f.read()

        # Split into individual queries (by semicolon)
        queries = [q.strip() for q in re.split(r";\s*", sql_content) if q.strip()]

        print(f"\n📜 Found {len(queries)} queries in {sql_file}\n")

        # Execute and display each query
        for i, query in enumerate(queries, start=1):
            print(f"🔹 Query {i}:")
            preview = " ".join(query.split()[:10]) + ("..." if len(query.split()) > 10 else "")
            print(indent(preview, "  "))
            print("   ─" * 25)

            try:
                cursor.execute(query)
                rows = cursor.fetchall()
                col_names = [desc[0] for desc in cursor.description] if cursor.description else []

                if not rows:
                    print("⚠️  No results.\n")
                    continue

                # Pretty print results
                print("📊 Results:")
                print(indent(" | ".join(col_names), "    "))
                print(indent("-" * (len(" | ".join(col_names)) + 4), ""))
                for row in rows[:10]:  # print only first 10 rows for readability
                    print(indent(" | ".join(str(c) for c in row), "    "))

                if len(rows) > 10:
                    print(indent(f"...({len(rows) - 10} more rows)", "    "))

            except sqlite3.Error as e:
                print(f"❌ Error executing query {i}: {e}")

            print("\n" + "=" * 60 + "\n")

        conn.close()
        print("✅ All queries executed successfully!")

    except Exception as e:
        print("❌ Unexpected error:", e)


if __name__ == "__main__":
    run_queries()
import sqlite3
conn = sqlite3.connect("movies.db")
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM movies;")
print("Movies loaded:", cursor.fetchone()[0])

cursor.execute("SELECT COUNT(*) FROM ratings;")
print("Ratings loaded:", cursor.fetchone()[0])

conn.close()
