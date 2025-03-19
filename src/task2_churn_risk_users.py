from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark_session(app_name="Task2_Churn_Risk_Analysis"):
    """
    Create and return a SparkSession instance.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_movie_data(spark, file_path):
    """
    Load movie ratings dataset from a CSV file into a Spark DataFrame.
    """
    schema_definition = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT, 
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING, 
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    return spark.read.csv(file_path, header=True, schema=schema_definition)

def detect_churn_risk_users(df):
    """
    Detect users at risk of churn based on canceled subscriptions and low watch time.
    """
    # Identify users with canceled subscriptions and less than 100 minutes of watch time
    churn_risk_data = df.filter((col("SubscriptionStatus") == "Canceled") & (col("WatchTime") < 100))
    
    # Count unique churn risk users
    churn_risk_user_count = churn_risk_data.select("UserID").distinct().count()
    
    # Get the total number of unique users in the dataset
    total_unique_users = df.select("UserID").distinct().count()
    
    # Prepare the result as a list of tuples to match the required format
    result_data = [
        ("Users with low watch time & canceled subscriptions", churn_risk_user_count)
    ]
    
    # Define the schema for the result DataFrame
    result_schema = StructType([
        StructField("Churn Risk Users", StringType(), False),
        StructField("Total Users", IntegerType(), False)
    ])
    
    # Create a DataFrame from the result data
    result_df = df.sparkSession.createDataFrame(result_data, result_schema)
    
    return result_df

def save_results_to_csv(result_df, output_path):
    """
    Save the resulting DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def run_task():
    """
    Main function to run the churn risk analysis task.
    """
    spark = create_spark_session()

    input_file_path = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-BandaSrija/input/movie_ratings_data.csv"
    output_file_path = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-BandaSrija/Outputs/churn_risk_users.csv"

    movie_data = load_movie_data(spark, input_file_path)
    churn_risk_result = detect_churn_risk_users(movie_data)  # Call function to identify churn risk
    save_results_to_csv(churn_risk_result, output_file_path)

    spark.stop()

if __name__ == "__main__":
    run_task()
