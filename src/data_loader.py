import pandas as pd
import ast
import os

class MovieDataLoader:
    def __init__(self,original_csv:str,processed_csv:str):
        self.original_csv = original_csv
        self.processed_csv = processed_csv

    def load_and_process(self):
        df = pd.read_csv(self.original_csv , encoding='utf-8' , on_bad_lines='skip').dropna()

        required_cols = {"title", "overview", "genres"}

        # Check for missing columns
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns in CSV file: {missing}")
        

        def extract_genre_names(genre_str):
            try:
                genre_list = ast.literal_eval(genre_str)  # safely parse the string to a Python list
                return ', '.join(d['name'] for d in genre_list if 'name' in d)
            except (ValueError, SyntaxError, TypeError):
                return ''  # return empty string if parsing fails

        # Apply the genre parser
        df['genres_clean'] = df['genres'].apply(extract_genre_names)

        # # Combine columns (optional)
        # df['combined_info'] = (
        #     "Title: " + df["title"] +
        #     " | Overview: " + df["overview"] +
        #     " | Genres: " + df["genres_clean"]
        # )

        # # Save the cleaned output
        # df[['title', 'overview', 'genres_clean', 'combined_info']].to_csv("processed_movies.csv", index=False, encoding='utf-8')

        df['combined_info'] = (
            "Title: " + df["title"] + " Overview: " +df["overview"] + "Genres : " + df["genres_clean"]
        )

        df[['combined_info']].to_csv(self.processed_csv , index=False,encoding='utf-8')

        return self.processed_csv

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    # Set input and output paths
    input_file = "movie_metadata.csv"
    output_file = "/Users/aishwaryagopalakrishnan/MOVIE_RECOMMENDER_SYSTEM_LLMOPS/data/processed_movies.csv"

    loader = MovieDataLoader("/Users/aishwaryagopalakrishnan/MOVIE_RECOMMENDER_SYSTEM_LLMOPS/data/movies_metadata.csv", output_file)
    output_file = loader.load_and_process()
    print(f"Processed file saved to: {output_file}")