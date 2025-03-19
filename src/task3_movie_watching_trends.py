from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def create_spark_session(app_name="Task3_Movie_Watching_Trends"):
    """
    Create and return a SparkSession instance.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_movie_ratings(spark, file_path):
    """
    Load movie ratings dataset from a CSV file into a Spark DataFrame.
    """
    schema_definition = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT, 
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING, 
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    return spark.read.csv(file_path, header=True, schema=schema_definition)

def extract_movie_trends(df):
    """
    Analyze the movie watching trends based on the number of movies watched each year.
    """
    # Group data by the year movies were watched and count the number of movies watched per year
    trends_by_year = df.groupBy("WatchedYear").agg(count("MovieID").alias("MoviesWatched"))
    
    # Order the results by the watched year for trend analysis
    trends_by_year = trends_by_year.orderBy("WatchedYear")
    
    # Rename columns to match the desired output format
    trends_by_year = trends_by_year.select(
        col("WatchedYear").alias("Watched Year"),
        col("MoviesWatched").alias("Movies Watched")
    )
    
    return trends_by_year

def save_trends_to_csv(result_df, output_path):
    """
    Save the resulting trend analysis DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def run_analysis():
    """
    Main function to execute Task 3 - Movie Watching Trends Analysis.
    """
    spark = create_spark_session()

    input_file_path = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-BandaSrija/input/movie_ratings_data.csv"
    output_file_path = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-BandaSrija/Outputs/movie_watching_trends.csv"

    movie_data = load_movie_ratings(spark, input_file_path)
    trend_analysis_result = extract_movie_trends(movie_data)  # Perform trend analysis
    save_trends_to_csv(trend_analysis_result, output_file_path)

    spark.stop()

if __name__ == "__main__":
    run_analysis()
