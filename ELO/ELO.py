from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col

# Calculate the expected score for a team based on ELO ratings
def calculate_expected_score(team_rating, opponent_rating):
    return 1 / (1 + 10 ** ((opponent_rating - team_rating) / 400))

# Update the ELO ratings after a match
def update_elo(winners, losers, ratings, k_factor=32, draw=False):
    winners_rating = sum(ratings.get(player, 1500) for player in winners) / len(winners)
    losers_rating = sum(ratings.get(player, 1500) for player in losers) / len(losers)
    expectation_winner = calculate_expected_score(winners_rating, losers_rating)

    if draw:
        # Handle draw: Both winners and losers are adjusted neutrally
        for player in winners:
            ratings[player] = ratings.get(player, 1500) + k_factor * (0.5 - expectation_winner)
        for player in losers:
            ratings[player] = ratings.get(player, 1500) + k_factor * (0.5 - (1 - expectation_winner))
    else:
        # Handle normal win/loss
        for player in winners:
            ratings[player] = ratings.get(player, 1500) + k_factor * (1 - expectation_winner)
        for player in losers:
            ratings[player] = ratings.get(player, 1500) + k_factor * (-1 + expectation_winner)

    return ratings

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ELO_Calculation").getOrCreate()

    # Paths to input and output files
    games_directory = "/user/s2185369/games_total/"
    games_played_file = "/user/s2163918/player_games_played"
    ratings_file = "/user/s2163918/ratings.parquet"
    output_file = "/user/s2163918/elo_results_test"

    # Read games played data
    try:
        games_played_df = spark.read.parquet(games_played_file)
        print(f"Loaded games played data from {games_played_file}.")
    except Exception as e:
        print(f"Error reading games played data: {e}")
        spark.stop()
        exit(1)

    # Get all files in the directory
    try:
        games_files = [
            f.getPath().toString() for f in spark._jvm.org.apache.hadoop.fs.FileSystem
            .get(spark._jsc.hadoopConfiguration())
            .listStatus(spark._jvm.org.apache.hadoop.fs.Path(games_directory))
        ]
        print(f"Found {len(games_files)} files in {games_directory}")
    except Exception as e:
        print(f"Error listing files in directory: {e}")
        games_files = []

    # Check if ratings file exists and load it, or create an empty ratings dictionary
    try:
        ratings_df = spark.read.parquet(ratings_file)
        ratings = {row["Player_ID"]: float(row["ELO"]) for row in ratings_df.collect()}
        print(f"Loaded ratings from {ratings_file}.")
    except AnalysisException:
        print(f"Ratings file not found at {ratings_file}. Initializing with default ratings.")
        ratings = {}  # Start with an empty dictionary

    # Process each games file
    for games_file in games_files:
        try:
            games_df = spark.read.parquet(games_file)

            # Process each game to update ELO ratings
            for row in games_df.collect():
                winners = row["winners"]
                losers = row["losers"]

                if row["draw"] == 1:
                    ratings = update_elo(winners, losers, ratings, draw=True)  # Draw handling
                else:
                    ratings = update_elo(winners, losers, ratings)
        except Exception as e:
            print(f"Error processing file {games_file}: {e}")

    # Save updated ratings back to HDFS
    if ratings:
        ratings_list = [(player, elo) for player, elo in ratings.items()]
        ratings_schema = ["Player_ID", "ELO"]
        ratings_df = spark.createDataFrame(ratings_list, schema=ratings_schema)

        # Filter ratings for players with at least 10 games played
        filtered_ratings_df = ratings_df.join(
            games_played_df.filter(col("Games_Played") >= 10),
            on="Player_ID",
            how="inner"
        )
        filtered_ratings_df.write.parquet(output_file, mode="overwrite")
        print(f"Filtered ELO ratings (players with ≥10 games) saved to {output_file}")
    else:
        print("No ratings were updated. No output file created.")
