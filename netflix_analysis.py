import pandas as pd
from tmdbv3api import TMDb, Movie
import time

tmdb = TMDb()
tmdb.api_key = "YOUR API KEY HERE"

movie_api = Movie()

def parse_mixed_dates(date_str):
    # Try parsing with dayfirst=True
    dt = pd.to_datetime(date_str, dayfirst=True, errors="coerce")
    if pd.isna(dt):
        # Fallback: try dayfirst=False
        dt = pd.to_datetime(date_str, dayfirst=False, errors="coerce")
    return dt


df = pd.read_csv("data/netflix_data.csv")


df["Date"] = df["Date"].apply(parse_mixed_dates)
df["Date"] = df["Date"].dt.strftime("%d/%m/%y")


invalid_dates = df[df["Date"].isna()]
if not invalid_dates.empty:
    print("\nRows with invalid date formats:\n", invalid_dates)


df["Genre"] = ""
df["Release_Year"] = ""
df["Country"] = ""
df["Runtime"] = ""

   
indexes_to_drop = []


for index, row in df.iterrows():
    title = row["Title"]
    found = False

    try:
        search_results = movie_api.search(title)
        if search_results:
            exact_matches = [
                r for r in search_results
                if hasattr(r, "title") and isinstance(r.title, str) and r.title.lower() == title.lower()
            ]
            movie = exact_matches[0] if exact_matches else sorted(
                search_results, key=lambda x: getattr(x, "popularity", 0), reverse=True
            )[0]

            details = movie_api.details(movie.id)

            df.at[index, "Genre"] = ", ".join([g["name"] for g in details.get("genres", [])])
            df.at[index, "Release_Year"] = details.get("release_date", "")[:4]
            df.at[index, "Country"] = ", ".join([c["iso_3166_1"] for c in details.get("production_countries", [])])
            df.at[index, "Runtime"] = details.get("runtime")
            found = True
        else:
            indexes_to_drop.append(index)

    except Exception as e:
        indexes_to_drop.append(index)

    time.sleep(0.25)

if indexes_to_drop:
    df.drop(index=indexes_to_drop, inplace=True)
    print(f"\nRemoved {len(indexes_to_drop)} rows not found as movies.")

missing_values = df.isna().sum()
print("\nMissing values per column:\n", missing_values)

df.to_csv("data/netflix_cleaned.csv", index=False)
print("\nCleaned dataset saved to netflix_cleaned.csv")
