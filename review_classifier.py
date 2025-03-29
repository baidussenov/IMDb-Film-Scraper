import pandas as pd
from ollama import Client
from pydantic import BaseModel
from typing import List, Dict
import json
import os
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from logging.handlers import RotatingFileHandler

# Initialize logging
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = RotatingFileHandler(
        'movie_analysis.log', 
        maxBytes=1024*1024,  # 1MB
        backupCount=3
    )
    
    # Set levels
    console_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.DEBUG)
    
    # Create formatters and add to handlers
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

setup_logging()
logger = logging.getLogger(__name__)

# Initialize Ollama client
client = Client(host='http://localhost:11434')

# Define Pydantic model for structured output
class ReviewAnalysis(BaseModel):
    strengths: List[str]
    weaknesses: List[str]

# Prompt template for Mistral (now includes JSON instruction)
PROMPT_TEMPLATE = """
You are an analyst extracting opinions from movie reviews. Given the following review title and content, identify the strengths and weaknesses as perceived by the reviewer. 

Return your response as a JSON object with two keys: "strengths" (list of strings) and "weaknesses" (list of strings). Only include opinions explicitly stated by the reviewer, do not add your own interpretations or assumptions. If no strengths or weaknesses are mentioned, return empty lists for those fields.

Review Title: {title}
Review Content: {content}

Respond ONLY with valid JSON matching this schema:
{json_schema}
"""

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def process_review(title, content):
    """Process a single review using structured output"""
    logger.debug(f"Processing review: {title[:50]}...")  # Log first 50 chars to avoid huge logs
    
    if pd.isna(content) or content.strip().lower() == 'n/a':
        logger.debug(f"Skipping empty review: {title}")
        return [], []
    
    # Get JSON schema from Pydantic model
    json_schema = ReviewAnalysis.model_json_schema()
    
    # Prepare prompt with schema
    prompt = PROMPT_TEMPLATE.format(
        title=title,
        content=content,
        json_schema=json.dumps(json_schema, indent=2)
    )
    
    try:
        # Get structured response from Mistral
        logger.debug(f"Sending prompt to Mistral for review: {title}")
        response = client.generate(
            model='mistral',
            prompt=prompt,
            format=json_schema,
            options={'temperature': 0}  # More deterministic output
        )
        
        # Parse and validate response
        try:
            analysis = ReviewAnalysis.model_validate_json(response['response'])
            logger.debug(f"Successfully processed review: {title}")
            return analysis.strengths, analysis.weaknesses
        except Exception as e:
            logger.error(f"Failed to parse response for '{title}': {str(e)}")
            logger.debug(f"Raw response: {response}")
            return [], []
    except Exception as e:
        logger.error(f"Error processing review '{title}': {str(e)}")
        raise  # This will trigger retry

def process_country_data(films_file, reviews_file):
    """Process data for a specific country"""
    country_name = os.path.basename(films_file).split('_')[0].title()
    logger.info(f"Starting processing for country: {country_name}")
    
    try:
        logger.debug(f"Loading films data from: {films_file}")
        films_df = pd.read_excel(films_file)
        logger.debug(f"Loading reviews data from: {reviews_file}")
        reviews_df = pd.read_excel(reviews_file)
    except Exception as e:
        logger.error(f"Failed to load data files for {country_name}: {str(e)}")
        raise
    
    review_groups = reviews_df.groupby('movie_title')
    results = []
    total_movies = len(films_df['title'])
    
    logger.info(f"Found {total_movies} movies to process for {country_name}")
    
    for i, movie_title in enumerate(films_df['title'], 1):
        logger.info(f"Processing movie {i}/{total_movies}: {movie_title}")
        
        if movie_title in review_groups.groups:
            reviews = review_groups.get_group(movie_title)
            logger.debug(f"Found {len(reviews)} reviews for {movie_title}")
            
            for _, review in reviews.iterrows():
                try:
                    s, w = process_review(
                        review['review_title'],
                        review['review_content']
                    )
                    
                    results.append({
                        'movie_title': movie_title,
                        'original_review_title': review['review_title'],
                        'original_review_content': review['review_content'],
                        'strengths': json.dumps(s),
                        'weaknesses': json.dumps(w)
                    })
                except Exception as e:
                    logger.error(f"Error processing review for {movie_title}: {str(e)}")
                    continue
        else:
            logger.debug(f"No reviews found for {movie_title}")
            results.append({
                'movie_title': movie_title,
                'original_review_title': None,
                'original_review_content': None,
                'strengths': json.dumps([]),
                'weaknesses': json.dumps([])
            })
    
    logger.info(f"Completed processing for {country_name}. Processed {len(results)} review entries.")
    return pd.DataFrame(results)

def main():
    logger.info("Starting movie analysis process")
    
    # File paths
    kz_films = 'tables/kazakhstan_films.xlsx'
    kz_reviews = 'tables/kazakhstan_reviews.xlsx'
    kr_films = 'tables/south_korea_films.xlsx'
    kr_reviews = 'tables/south_korea_reviews.xlsx'
    
    # Verify files exist
    required_files = [kz_films, kz_reviews, kr_films, kr_reviews]
    for file in required_files:
        if not os.path.exists(file):
            error_msg = f"Required file '{file}' not found"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        logger.debug(f"File found: {file}")
    
    try:
        # Process both countries and save separately
        logger.info("Processing Kazakhstan data")
        kz_results = process_country_data(kz_films, kz_reviews)
        
        logger.info("Processing South Korea data")
        kr_results = process_country_data(kr_films, kr_reviews)
        
        # Save results separately
        kz_output = 'tables/kazakhstan_movie_analysis.xlsx'
        kr_output = 'tables/south_korea_movie_analysis.xlsx'
        
        logger.info(f"Saving Kazakhstan results to {kz_output}")
        kz_results.to_excel(kz_output, index=False)
        
        logger.info(f"Saving South Korea results to {kr_output}")
        kr_results.to_excel(kr_output, index=False)
        
        logger.info("Analysis complete! Results saved successfully.")
        print("Analysis complete! Results saved separately for each country.")
        
    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Application failed: {str(e)}", exc_info=True)
        print(f"Error: {str(e)}")