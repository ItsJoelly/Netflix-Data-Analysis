import pandas as pd
from tmdbv3api import TMDb, Movie
import time

tmdb = TMDb()
tmdb.api_key = "eb1d7e994fbb6bf11bc2920ab1620834"

movie_api = Movie()
tv_api = TV()

df = pd.read_csv("netflix_data.csv")

df['Genre'] = ''
df['Release_Year'] = ''
df['Country'] = ''
df['Rating'] = ''
df['Vote_Average'] = ''
df['Vote_Count'] = ''
df['Popularity'] = ''
df['Type'] = ''
df['Runtime'] = ''


df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
df['Date'] = df['Date'].dt.strftime('%d/%m/%y')
invalid_dates = df[df['Date'].isna()]
if not invalid_dates.empty:
    print("\nRows with invalid date formats:\n", invalid_dates)


for index, row in df.iterrows():
    title = row['Title']
    found = False

    try:
        # First try as movie
        search_results = movie_api.search(title)
        if search_results:
            movie = search_results[0]
            details = movie.details()
            df.at[index, 'Type'] = 'Movie'
            df.at[index, 'Genre'] = ', '.join([g['name'] for g in details.get('genres', [])])
            df.at[index, 'Release_Year'] = details.get('release_date', '')[:4]
            df.at[index, 'Country'] = ', '.join([c['iso_3166_1'] for c in details.get('production_countries', [])])
            df.at[index, 'Rating'] = details.get('certification', '')  # might be empty
            df.at[index, 'Vote_Average'] = details.get('vote_average')
            df.at[index, 'Vote_Count'] = details.get('vote_count')
            df.at[index, 'Popularity'] = details.get('popularity')
            df.at[index, 'Runtime'] = details.get('runtime')
            found = True

        # If not movie, try as TV show
        if not found:
            search_results = tv_api.search(title)
            if search_results:
                tv_show = search_results[0]
                details = tv_show.details()
                df.at[index, 'Type'] = 'TV Show'
                df.at[index, 'Genre'] = ', '.join([g['name'] for g in details.get('genres', [])])
                df.at[index, 'Release_Year'] = details.get('first_air_date', '')[:4]
                df.at[index, 'Country'] = ', '.join(details.get('origin_country', []))
                df.at[index, 'Rating'] = ''  # Ratings not always provided
                df.at[index, 'Vote_Average'] = details.get('vote_average')
                df.at[index, 'Vote_Count'] = details.get('vote_count')
                df.at[index, 'Popularity'] = details.get('popularity')
                df.at[index, 'Runtime'] = ''  # Runtime varies by episode
                found = True

    except Exception as e:
        print(f"Error processing '{title}': {e}")

    # Respect TMDb's rate limits
    time.sleep(0.25)

print("\nMissing values per column:\n", missing_values)
df.to_csv("netflix_cleaned.csv", index=False)
print("\nCleaned dataset saved to 'netflix_cleaned.csv'")

