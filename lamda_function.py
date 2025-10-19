import pandas as pd
import requests
import re
import boto3
from bs4 import BeautifulSoup
from io import StringIO
import os

# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------
OMDB_API_KEY = os.environ.get("OMDB_API_KEY", "replace api")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "your s3 bucket name ")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; AWSLambdaBot/1.0; +https://aws.amazon.com/lambda/)'
}

# ----------------------------------------------------------------------
# UTILITIES
# ----------------------------------------------------------------------
def remove_references(text):
    """Remove Wikipedia-style reference tags."""
    if isinstance(text, str):
        return re.sub(r'\[.*?\]', '', text).strip()
    return text

def find_film_column(columns):
    """Finds a column likely to represent the film title."""
    for col in columns:
        if re.search(r'(?i)film|title', str(col)):
            return col
    return None

# ----------------------------------------------------------------------
# STEP 1: SCRAPE MCU MOVIES
# ----------------------------------------------------------------------
def scrape_marvel_movies():
    url = "https://en.wikipedia.org/wiki/List_of_Marvel_Cinematic_Universe_films"
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.content, 'html.parser')

    tables = soup.find_all('table', {'class': 'wikitable'})
    film_tables = []

    for tbl in tables:
        caption = tbl.find('caption')
        if caption and re.search(r'(?i)phase', caption.get_text()):
            headers = [th.get_text(strip=True) for th in tbl.find_all('tr')[0].find_all('th')]
            rows = []
            for tr in tbl.find_all('tr')[1:]:
                cols = [td.get_text(" ", strip=True) for td in tr.find_all(['th', 'td'])]
                if len(cols) == len(headers):
                    rows.append(cols)
            if rows:
                df = pd.DataFrame(rows, columns=headers)
                film_tables.append(df)

    if not film_tables:
        raise Exception("No MCU film tables found on Wikipedia page.")

    # Combine all film tables
    movies_df = pd.concat(film_tables, ignore_index=True)

    # Normalize column names
    movies_df.columns = [c.strip().replace('\xa0', ' ') for c in movies_df.columns]

    # Detect film/title column dynamically
    film_col = find_film_column(movies_df.columns)
    if not film_col:
        raise Exception(f"Could not find any 'film' or 'title' column. Found columns: {movies_df.columns.tolist()}")

    movies_df = movies_df.rename(columns={film_col: 'film'})

    # Optional renames for consistency
    rename_map = {
        'U.S. release date': 'release_date',
        'Release date': 'release_date',
        'Director(s)': 'director',
        'Directed by': 'director',
        'Screenwriter(s)': 'writer',
        'Writer(s)': 'writer',
        'Producer(s)': 'producer',
        'Produced by': 'producer'
    }
    movies_df = movies_df.rename(columns=rename_map)

    movies_df['film'] = movies_df['film'].apply(remove_references)

    print(f"✅ Scraped {len(movies_df)} Marvel films ({len(film_tables)} phases).")
    return movies_df

# ----------------------------------------------------------------------
# STEP 2: CLEAN MOVIE DATA
# ----------------------------------------------------------------------
def clean_movie_data(movies_df):
    movies_df_cleaned = movies_df.copy()

    if 'release_date' in movies_df_cleaned.columns:
        extracted_dates = movies_df_cleaned['release_date'].str.extract(r'\((.*?)\)')[0]
        movies_df_cleaned['release_date'] = extracted_dates.fillna(movies_df_cleaned['release_date'])

    for col in movies_df_cleaned.select_dtypes(include=['object']).columns:
        movies_df_cleaned[col] = movies_df_cleaned[col].map(remove_references)

    return movies_df_cleaned.dropna(subset=['film'])

# ----------------------------------------------------------------------
# STEP 3: SCRAPE CHARACTERS TABLE
# ----------------------------------------------------------------------
def scrape_characters_data():
    url = "https://en.wikipedia.org/wiki/List_of_Marvel_Cinematic_Universe_films"
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = None

    for tbl in soup.find_all('table', class_='wikitable'):
        caption = tbl.find('caption')
        if caption and "Recurring cast and characters" in caption.get_text():
            table = tbl
            break

    if not table:
        print("⚠️ No characters table found.")
        return pd.DataFrame()

    headers = [th.get_text(strip=True) for th in table.find('tr').find_all('th')]
    rows = []
    for tr in table.find_all('tr')[1:]:
        cols = [td.get_text(" ", strip=True) for td in tr.find_all(['th', 'td'])]
        while len(cols) < len(headers):
            cols.append(None)
        rows.append(cols)
    df = pd.DataFrame(rows, columns=headers)
    print(f"✅ Scraped {len(df)} character/cast entries.")
    return df

# ----------------------------------------------------------------------
# STEP 4: FETCH OMDB DATA
# ----------------------------------------------------------------------
def fetch_omdb_data(film_name):
    url = f'http://www.omdbapi.com/?t={film_name}&apikey={OMDB_API_KEY}'
    try:
        res = requests.get(url)
        return res.json() if res.status_code == 200 else {"Title": film_name, "Error": "Request failed"}
    except Exception as e:
        return {"Title": film_name, "Error": str(e)}

# ----------------------------------------------------------------------
# STEP 5: UPLOAD TO S3
# ----------------------------------------------------------------------
def upload_to_s3(df, file_name):
    if df.empty:
        print(f"⚠️ Skipping upload for {file_name}: empty DataFrame.")
        return
    s3 = boto3.client('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_key = f"etl_output/{file_name}"
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"✅ Uploaded {file_name} to s3://{S3_BUCKET_NAME}/{s3_key}")

# ----------------------------------------------------------------------
# MAIN HANDLER
# ----------------------------------------------------------------------
def lambda_handler(event=None, context=None):
    print(f"🚀 Starting MCU ETL (OMDB Key: {OMDB_API_KEY}, Bucket: {S3_BUCKET_NAME})")

    try:
        movies_df = scrape_marvel_movies()
    except Exception as e:
        print(f"❌ ERROR scraping MCU data: {e}")
        return {'statusCode': 500, 'body': "Failed to scrape Marvel movie data."}

    movies_df_cleaned = clean_movie_data(movies_df)
    print(f"✅ Cleaned movies: {len(movies_df_cleaned)}")

    omdb_data = [fetch_omdb_data(title) for title in movies_df_cleaned['film']]
    omdb_df = pd.DataFrame(omdb_data)
    print(f"✅ OMDB data collected for {len(omdb_df)} films")

    characters_df = scrape_characters_data()

    upload_to_s3(movies_df_cleaned, 'movies_cleaned.csv')
    upload_to_s3(omdb_df, 'omdb_data.csv')
    upload_to_s3(characters_df, 'characters_cast.csv')

    return {'statusCode': 200, 'body': 'ETL completed successfully'}

if __name__ == "__main__":
    lambda_handler()
