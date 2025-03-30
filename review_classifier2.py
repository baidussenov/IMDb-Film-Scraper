import pandas as pd
from ollama import Client
from pydantic import BaseModel
from typing import List, Dict, Optional
import json
import os
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from logging.handlers import RotatingFileHandler

# Initialize logging
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    file_handler = RotatingFileHandler('movie_analysis.log', maxBytes=1024*1024, backupCount=3)
    console_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

setup_logging()
logger = logging.getLogger(__name__)

# Initialize Ollama client
client = Client(host='http://localhost:11434')

# Predefined categories with descriptions
STRENGTH_CATEGORIES = {
    "Acting/Performances": "Praise for actors' performances",
    "Story/Screenplay": "Quality of narrative, plot, dialogue",
    "Direction": "Filmmaking skill, vision",
    "Cinematography": "Visual style, camera work",
    "Production Design": "Sets, costumes, visual world-building",
    "Music/Soundtrack": "Score, sound design",
    "Emotional Impact": "Ability to evoke emotions",
    "Originality": "Freshness, creativity",
    "Pacing": "Narrative flow, rhythm",
    "Cultural Significance": "Representation, social relevance"
}

WEAKNESS_CATEGORIES = {
    "Weak Acting": "Poor performances",
    "Plot Issues": "Holes, inconsistencies",
    "Poor Direction": "Lack of vision, execution",
    "Technical Flaws": "Editing, sound issues",
    "Pacing Problems": "Too slow/fast",
    "Predictability": "Lack of surprises",
    "Character Development": "Underdeveloped characters",
    "Tonal Issues": "Inconsistent mood",
    "Excessive Length": "Overly long runtime",
    "Cultural Missteps": "Offensive or insensitive elements"
}

# Define Pydantic model for structured output
class SuggestedCategory(BaseModel):
    title: str
    description: str

class ReviewAnalysis(BaseModel):
    strengths: List[str]
    weaknesses: List[str]
    suggested_categories: Optional[List[SuggestedCategory]] = None

# Updated Prompt template
PROMPT_TEMPLATE = """
You are an analyst extracting opinions from movie reviews. Given the following review title and content, categorize the strengths and weaknesses into the predefined categories below. Only include categories explicitly supported by the review text. Use ONLY the category titles in "strengths" and "weaknesses" lists, not their descriptions. If the predefined categories aren't sufficient, suggest new atomic and generalizable categories under "suggested_categories", providing both a title and a brief description for each. Ensure suggested categories match any new titles used in "strengths" or "weaknesses".

Predefined Strength Categories (title: description):
{strength_cats}

Predefined Weakness Categories (title: description):
{weakness_cats}

Review Title: {title}
Review Content: {content}

Return ONLY valid JSON matching this schema:
{json_schema}
"""

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def process_review(title, content):
    """Process a single review using structured output with predefined categories"""
    global STRENGTH_CATEGORIES, WEAKNESS_CATEGORIES

    logger.debug(f"Processing review: {title[:50]}...")
    
    if pd.isna(content) or content.strip().lower() == 'n/a':
        logger.debug(f"Skipping empty review: {title}")
        return [], [], []
    
    json_schema = ReviewAnalysis.model_json_schema()
    
    strength_cats_str = "\n".join([f"{k}: {v}" for k, v in STRENGTH_CATEGORIES.items()])
    weakness_cats_str = "\n".join([f"{k}: {v}" for k, v in WEAKNESS_CATEGORIES.items()])
    
    prompt = PROMPT_TEMPLATE.format(
        title=title,
        content=content,
        strength_cats=strength_cats_str,
        weakness_cats=weakness_cats_str,
        json_schema=json.dumps(json_schema, indent=2)
    )
    
    try:
        logger.debug(f"Sending prompt to the language model for review: {title}")
        response = client.generate(
            model='deepseek-r1:32b',
            prompt=prompt,
            format=json_schema,
            options={'temperature': 0}
        )
        
        try:
            analysis = ReviewAnalysis.model_validate_json(response['response'])
            
            # Handle suggested categories first
            suggested = analysis.suggested_categories or []
            suggested_titles = []
            if suggested:
                for cat in suggested:
                    # Determine if it's a strength or weakness based on presence in original lists
                    is_strength = cat.title in analysis.strengths
                    is_weakness = cat.title in analysis.weaknesses
                    if cat.title not in STRENGTH_CATEGORIES and cat.title not in WEAKNESS_CATEGORIES:
                        if is_strength and not is_weakness:
                            STRENGTH_CATEGORIES[cat.title] = cat.description
                            logger.info(f"New strength category added: {cat.title} - {cat.description}")
                        elif is_weakness and not is_strength:
                            WEAKNESS_CATEGORIES[cat.title] = cat.description
                            logger.info(f"New weakness category added: {cat.title} - {cat.description}")
                        else:
                            # Default to strength if unclear or used in both
                            STRENGTH_CATEGORIES[cat.title] = cat.description
                            logger.info(f"New category (assumed strength) added: {cat.title} - {cat.description}")
                        logger.info(f"Updated strength categories: {list(STRENGTH_CATEGORIES.keys())}")
                        logger.info(f"Updated weakness categories: {list(WEAKNESS_CATEGORIES.keys())}")
                    suggested_titles.append(cat.title)
            
            # Validate and include newly added categories
            valid_strengths = list(STRENGTH_CATEGORIES.keys())
            valid_weaknesses = list(WEAKNESS_CATEGORIES.keys())
            strengths = [s for s in analysis.strengths if s in valid_strengths]
            weaknesses = [w for w in analysis.weaknesses if w in valid_weaknesses]
            
            if len(strengths) != len(analysis.strengths) or len(weaknesses) != len(analysis.weaknesses):
                logger.warning(f"Invalid categories filtered out for review: {title}")
            
            logger.debug(f"Successfully processed review: {title}")
            return strengths, weaknesses, suggested_titles
        except Exception as e:
            logger.error(f"Failed to parse response for '{title}': {str(e)}")
            logger.debug(f"Raw response: {response}")
            return [], [], []
    except Exception as e:
        logger.error(f"Error processing review '{title}': {str(e)}")
        raise

def process_country_data(films_file, reviews_file):
    """Process data for a specific country"""
    country_name = os.path.basename(films_file).split('_')[0].title()
    logger.info(f"Starting processing for country: {country_name}")
    
    try:
        films_df = pd.read_excel(films_file)
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
                    s, w, suggested = process_review(
                        review['review_title'],
                        review['review_content']
                    )
                    
                    results.append({
                        'movie_title': movie_title,
                        'original_review_title': review['review_title'],
                        'original_review_content': review['review_content'],
                        'strengths': json.dumps(s),
                        'weaknesses': json.dumps(w),
                        'suggested_categories': json.dumps(suggested)
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
                'weaknesses': json.dumps([]),
                'suggested_categories': json.dumps([])
            })
    
    logger.info(f"Completed processing for {country_name}. Processed {len(results)} review entries.")
    return pd.DataFrame(results)

def main():
    logger.info("Starting movie analysis process")
    
    kz_films = 'tables/kazakhstan_films.xlsx'
    kz_reviews = 'tables/kazakhstan_reviews.xlsx'
    kr_films = 'tables/south_korea_films.xlsx'
    kr_reviews = 'tables/south_korea_reviews.xlsx'
    
    required_files = [kz_films, kz_reviews, kr_films, kr_reviews]
    for file in required_files:
        if not os.path.exists(file):
            error_msg = f"Required file '{file}' not found"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        logger.debug(f"File found: {file}")
    
    try:
        kz_output = 'tables/kazakhstan_movie_analysis_categorized.xlsx'
        kr_output = 'tables/south_korea_movie_analysis_categorized.xlsx'

        logger.info("Processing Kazakhstan data")
        kz_results = process_country_data(kz_films, kz_reviews)
        logger.info(f"Saving Kazakhstan results to {kz_output}")
        kz_results.to_excel(kz_output, index=False)
        
        logger.info("Processing South Korea data")
        kr_results = process_country_data(kr_films, kr_reviews)
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