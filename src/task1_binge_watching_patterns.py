from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round as pyspark_round

def create_spark_session(app_name="Task1_Binge_Watching_Patterns"):
    """
    Creates and returns a SparkSession instance.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_movie_data(spark, file_path):
    """
    Load the movie ratings dataset from the specified CSV file into a Spark DataFrame.
    """
    schema_definition = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT, 
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING, 
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    return spark.read.csv(file_path, header=True, schema=schema_definition)

def analyze_binge_watching(df):
    """
    Calculate the percentage of binge watchers in each age group.

    Steps:
    1. Filter users with `IsBingeWatched = True`.
    2. Count binge-watchers and total users in each age group.
    3. Calculate the binge-watching percentage for each group.
    """
    # Get total number of unique users per age group
    total_users_by_group = df.select("UserID", "AgeGroup").dropDuplicates().groupBy("AgeGroup").count().withColumnRenamed("count", "TotalUsers")
    
    # Get the count of binge-watchers in each age group
    binge_watchers_by_group = df.filter(col("IsBingeWatched") == True).select("UserID", "AgeGroup").dropDuplicates().groupBy("AgeGroup").count().withColumnRenamed("count", "BingeWatchers")
    
    # Combine the two DataFrames and calculate the percentage
    merged_data = binge_watchers_by_group.join(total_users_by_group, "AgeGroup")
    merged_data = merged_data.withColumn("Percentage", pyspark_round((col("BingeWatchers") / col("TotalUsers") * 100), 2))
    
    # Select and reorder columns for final output
    final_result = merged_data.select("AgeGroup", "BingeWatchers", "Percentage")
    
    return final_result

def save_binge_watch_results(df, output_path):
    """
    Save the output DataFrame to a CSV file.
    """
    df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def execute_task():
    """
    Main function to run the binge-watching analysis task.
    """
    spark = create_spark_session()

    input_data_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-BandaSrija/input/movie_ratings_data.csv"
    output_data_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-BandaSrija/Outputs/binge_watching_patterns.csv"

    movie_data = load_movie_data(spark, input_data_file)
    result_data = analyze_binge_watching(movie_data)  # Perform analysis here
    save_binge_watch_results(result_data, output_data_file)

    spark.stop()

if __name__ == "__main__":
    execute_task()
