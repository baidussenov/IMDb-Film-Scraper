import pandas as pd
import json
import numpy as np
from ast import literal_eval
import re
from datetime import datetime

# Yearly currency conversion rates (2000-2025)
YEARLY_CURRENCY_RATES = {
    # Format: {year: {currency_symbol: rate_to_usd}}
    2000: {'$': 1.0, '€': 0.923, 'RUR': 0.0355, 'BND': 0.581, '₩': 0.000885, 'KZT': 0.00704},
    2001: {'$': 1.0, '€': 0.893, 'RUR': 0.0343, 'BND': 0.558, '₩': 0.000775, 'KZT': 0.00681},
    2002: {'$': 1.0, '€': 0.945, 'RUR': 0.0319, 'BND': 0.559, '₩': 0.000800, 'KZT': 0.00652},
    2003: {'$': 1.0, '€': 1.131, 'RUR': 0.0326, 'BND': 0.574, '₩': 0.000839, 'KZT': 0.00667},
    2004: {'$': 1.0, '€': 1.244, 'RUR': 0.0347, 'BND': 0.592, '₩': 0.000873, 'KZT': 0.00735},
    2005: {'$': 1.0, '€': 1.245, 'RUR': 0.0353, 'BND': 0.598, '₩': 0.000977, 'KZT': 0.00751},
    2006: {'$': 1.0, '€': 1.256, 'RUR': 0.0368, 'BND': 0.630, '₩': 0.001047, 'KZT': 0.00793},
    2007: {'$': 1.0, '€': 1.371, 'RUR': 0.0391, 'BND': 0.664, '₩': 0.001076, 'KZT': 0.00819},
    2008: {'$': 1.0, '€': 1.471, 'RUR': 0.0402, 'BND': 0.709, '₩': 0.000909, 'KZT': 0.00833},
    2009: {'$': 1.0, '€': 1.395, 'RUR': 0.0316, 'BND': 0.688, '₩': 0.000786, 'KZT': 0.00676},
    2010: {'$': 1.0, '€': 1.326, 'RUR': 0.0329, 'BND': 0.736, '₩': 0.000865, 'KZT': 0.00678},
    2011: {'$': 1.0, '€': 1.392, 'RUR': 0.0340, 'BND': 0.795, '₩': 0.000902, 'KZT': 0.00683},
    2012: {'$': 1.0, '€': 1.286, 'RUR': 0.0323, 'BND': 0.800, '₩': 0.000888, 'KZT': 0.00671},
    2013: {'$': 1.0, '€': 1.328, 'RUR': 0.0314, 'BND': 0.798, '₩': 0.000915, 'KZT': 0.00656},
    2014: {'$': 1.0, '€': 1.329, 'RUR': 0.0261, 'BND': 0.789, '₩': 0.000948, 'KZT': 0.00549},
    2015: {'$': 1.0, '€': 1.110, 'RUR': 0.0164, 'BND': 0.727, '₩': 0.000884, 'KZT': 0.00451},
    2016: {'$': 1.0, '€': 1.107, 'RUR': 0.0149, 'BND': 0.724, '₩': 0.000861, 'KZT': 0.00292},
    2017: {'$': 1.0, '€': 1.130, 'RUR': 0.0172, 'BND': 0.725, '₩': 0.000886, 'KZT': 0.00299},
    2018: {'$': 1.0, '€': 1.181, 'RUR': 0.0159, 'BND': 0.743, '₩': 0.000906, 'KZT': 0.00290},
    2019: {'$': 1.0, '€': 1.120, 'RUR': 0.0155, 'BND': 0.735, '₩': 0.000858, 'KZT': 0.00261},
    2020: {'$': 1.0, '€': 1.142, 'RUR': 0.0138, 'BND': 0.726, '₩': 0.000845, 'KZT': 0.00242},
    2021: {'$': 1.0, '€': 1.183, 'RUR': 0.0136, 'BND': 0.744, '₩': 0.000873, 'KZT': 0.00235},
    2022: {'$': 1.0, '€': 1.053, 'RUR': 0.0145, 'BND': 0.725, '₩': 0.000771, 'KZT': 0.00217},
    2023: {'$': 1.0, '€': 1.081, 'RUR': 0.0117, 'BND': 0.747, '₩': 0.000767, 'KZT': 0.00219},
    2024: {'$': 1.0, '€': 1.085, 'RUR': 0.0109, 'BND': 0.737, '₩': 0.000736, 'KZT': 0.00223},
    2025: {'$': 1.0, '€': 1.12, 'RUR': 0.0115, 'BND': 0.71, '₩': 0.000800, 'KZT': 0.0025},  # Speculative, retained as provided
    # Default values updated to a 2020s baseline
    'default': {'$': 1.0, '€': 1.10, 'RUR': 0.012, 'BND': 0.74, '₩': 0.00080, 'KZT': 0.0022}
}

def clean_genres(genre_str):
    """Clean and parse the genres column"""
    if pd.isna(genre_str):
        return []
    
    cleaned = genre_str.strip("[]").split(",")
    genres = []
    for g in cleaned:
        g_clean = g.strip().strip("'").strip('"')
        if g_clean and "Back to top" not in g_clean:
            genres.append(g_clean)
    
    return genres

def parse_list_column(x):
    """Parse JSON or Python list strings"""
    if pd.isna(x) or x == '[]':
        return []
    try:
        return json.loads(x)
    except json.JSONDecodeError:
        try:
            return literal_eval(x)
        except:
            return []

def get_conversion_rate(currency_symbol, year):
    """Get conversion rate for a currency in a specific year"""
    rates = YEARLY_CURRENCY_RATES.get(year, YEARLY_CURRENCY_RATES['default'])
    return rates.get(currency_symbol, None)

def convert_currency(value, year):
    """Convert currency values to USD if rate is available"""
    if pd.isna(value) or value == '':
        return None
    
    value = re.sub(r'\(estimated\)', '', value, flags=re.IGNORECASE).strip()
    match = re.match(r'^([^\d]*)([\d,]+\.?\d*)$', value.strip())
    if not match:
        return None
    
    currency_symbol = match.group(1).strip()
    amount_str = match.group(2).replace(',', '')
    
    try:
        amount = float(amount_str)
    except ValueError:
        return None
    
    conversion_rate = get_conversion_rate(currency_symbol, year)
    if conversion_rate is None:
        return None
    
    return amount * conversion_rate

def process_movies(movies_df, country):
    """Create movies sheet expanded by genre"""
    movies_df = movies_df.copy()
    movies_df['country'] = country
    
    if 'year' in movies_df.columns:
        for col in ['local_gross', 'worldwide_gross', 'budget', 'opening_weekend']:
            if col in movies_df.columns:
                movies_df[col] = movies_df.apply(
                    lambda row: convert_currency(row[col], row['year']) if pd.notna(row['year']) else None,
                    axis=1
                )
    
    movies_df['genres'] = movies_df['genres'].apply(clean_genres)
    movies_expanded = movies_df.explode('genres')
    movies_expanded = movies_expanded.rename(columns={'genres': 'genre'})
    
    cols = ['movie_title', 'year'] + [col for col in movies_expanded.columns 
                                     if col not in ['movie_title', 'year']]
    return movies_expanded[cols]

def process_reviews_strengths(reviews_df, categorized_df, country, movies_df):
    """Create reviews sheet expanded by strengths"""
    # Get movie years from movies data
    movie_years = movies_df[['movie_title', 'year']].drop_duplicates()
    
    merged = pd.merge(
        reviews_df,
        categorized_df,
        left_on=['movie_title', 'review_title', 'review_content'],
        right_on=['movie_title', 'original_review_title', 'original_review_content'],
        how='left'
    )
    
    # Add movie year from movies data
    merged = pd.merge(merged, movie_years, on='movie_title', how='left')
    
    merged['country'] = country
    merged['strengths'] = merged['strengths'].apply(parse_list_column)
    expanded = merged.explode('strengths')
    
    if 'date' in expanded.columns:
        expanded['review_year'] = pd.to_datetime(expanded['date']).dt.year
    
    columns_to_keep = [
        'movie_title', 'year', 'review_year', 'country', 'review_score', 
        'review_title', 'review_content', 'upvotes', 'downvotes', 'date',
        'permalink', 'strengths'
    ]
    
    available_columns = [col for col in columns_to_keep if col in expanded.columns]
    result = expanded[available_columns].rename(columns={
        'upvotes': 'review_upvotes',
        'downvotes': 'review_downvotes',
        'date': 'review_date',
        'permalink': 'review_url',
        'strengths': 'strength'
    })
    
    if 'review_score' in result.columns:
        result['review_score'] = result['review_score'].replace('N/A', '')
    if 'review_content' in result.columns:
        result['review_content'] = result['review_content'].replace('N/A', '')
    
    return result

def process_reviews_weaknesses(reviews_df, categorized_df, country, movies_df):
    """Create reviews sheet expanded by weaknesses"""
    # Get movie years from movies data
    movie_years = movies_df[['movie_title', 'year']].drop_duplicates()
    
    merged = pd.merge(
        reviews_df,
        categorized_df,
        left_on=['movie_title', 'review_title', 'review_content'],
        right_on=['movie_title', 'original_review_title', 'original_review_content'],
        how='left'
    )
    
    # Add movie year from movies data
    merged = pd.merge(merged, movie_years, on='movie_title', how='left')
    
    merged['country'] = country
    merged['weaknesses'] = merged['weaknesses'].apply(parse_list_column)
    expanded = merged.explode('weaknesses')
    
    if 'date' in expanded.columns:
        expanded['review_year'] = pd.to_datetime(expanded['date']).dt.year
    
    columns_to_keep = [
        'movie_title', 'year', 'review_year', 'country', 'review_score', 
        'review_title', 'review_content', 'upvotes', 'downvotes', 'date',
        'permalink', 'weaknesses'
    ]
    
    available_columns = [col for col in columns_to_keep if col in expanded.columns]
    result = expanded[available_columns].rename(columns={
        'upvotes': 'review_upvotes',
        'downvotes': 'review_downvotes',
        'date': 'review_date',
        'permalink': 'review_url',
        'weaknesses': 'weakness'
    })
    
    if 'review_score' in result.columns:
        result['review_score'] = result['review_score'].replace('N/A', '')
    if 'review_content' in result.columns:
        result['review_content'] = result['review_content'].replace('N/A', '')
    
    return result

def main():
    input_path = 'tables/Primary Research.xlsx'
    output_path = 'tables/merged.xlsx'
    
    xls = pd.ExcelFile(input_path)
    
    # Process both countries
    kaz_movies = pd.read_excel(xls, 'KAZ movies').rename(columns={'title': 'movie_title'})
    kaz_reviews = pd.read_excel(xls, 'KAZ reviews')
    kaz_categorized = pd.read_excel(xls, 'KAZ reviews categorized')
    
    kor_movies = pd.read_excel(xls, 'KOR movies').rename(columns={'title': 'movie_title'})
    kor_reviews = pd.read_excel(xls, 'KOR reviews')
    kor_categorized = pd.read_excel(xls, 'KOR reviews categorized')
    
    # Process and combine data
    kaz_movies_processed = process_movies(kaz_movies, 'KAZ')
    kor_movies_processed = process_movies(kor_movies, 'KOR')
    movies_combined = pd.concat([kaz_movies_processed, kor_movies_processed])
    
    strengths_combined = pd.concat([
        process_reviews_strengths(kaz_reviews, kaz_categorized, 'KAZ', kaz_movies_processed),
        process_reviews_strengths(kor_reviews, kor_categorized, 'KOR', kor_movies_processed)
    ])
    
    weaknesses_combined = pd.concat([
        process_reviews_weaknesses(kaz_reviews, kaz_categorized, 'KAZ', kaz_movies_processed),
        process_reviews_weaknesses(kor_reviews, kor_categorized, 'KOR', kor_movies_processed)
    ])
    
    # Save to Excel
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        movies_combined.to_excel(writer, sheet_name='Movies by Genre', index=False)
        strengths_combined.to_excel(writer, sheet_name='Reviews by Strengths', index=False)
        weaknesses_combined.to_excel(writer, sheet_name='Reviews by Weaknesses', index=False)
    
    print(f"Output saved to {output_path} with 3 sheets")

if __name__ == '__main__':
    main()