from pyspark.sql import SparkSession
import sys

# Calculate the expected score for a team based on ELO ratings
def calculate_expected_score(team_rating, opponent_rating):
    # ELO formula: Expected score = 1 / (1 + 10 ^ ((OpponentRating - TeamRating) / 400))
    return 1 / (1 + 10 ** ((opponent_rating - team_rating) / 400))

# Update the ELO ratings after a match
def update_elo(winners, losers, ratings, k_factor=32, draw=False):
    # Calculate the average ELO rating for the teams
    winners_rating = sum(ratings.get(player, 1500) for player in winners) / len(winners)
    losers_rating = sum(ratings.get(player, 1500) for player in losers) / len(losers)

    # Calculate if it was expected that this team won based on the ELO scores
    expectation_winner = calculate_expected_score(winners_rating, losers_rating)

    if draw:
        # Update ELO ratings for a draw
        for player in winners:
            ratings[player] = ratings.get(player, 1500) + k_factor * (0.5 - expectation_winner)
        for player in losers:
            ratings[player] = ratings.get(player, 1500) + k_factor * (0.5 - (1 - expectation_winner))
    else:
        # Update ELO ratings for a win/loss
        for player in winners:
            ratings[player] = ratings.get(player, 1500) + k_factor * (1 - expectation_winner)
        for player in losers:
            ratings[player] = ratings.get(player, 1500) + k_factor * (-1 + expectation_winner)

    return ratings

if __name__ == "__main__":
    # Initialize Spark session for distributed processing
    spark = SparkSession.builder.appName("ELO").getOrCreate()

    # Paths for input files in HDFS (command-line arguments)
    games_file = sys.argv[1]  # Path to the games.parquet file in HDFS
    ratings_file = sys.argv[2]  # Path to the ratings.parquet file in HDFS

    # Load player ratings from HDFS into a dictionary
    ratings_df = spark.read.parquet(ratings_file)
    ratings = {row["Player_ID"]: float(row["ELO"]) for row in ratings_df.collect()}

    # Load game data from HDFS into a DataFrame
    games_df = spark.read.parquet(games_file)

    # Process each game chronologically to update ratings
    for row in games_df.collect():
        game_id = row["Game_ID"]  # Game identifier

        # Use arrays for Winners and Losers
        winners = row["Winners"]  # Assume 'Winners' is a list or array in the Parquet file
        losers = row["Losers"]    # Assume 'Losers' is a list or array in the Parquet file

        if row["Draw"] == 1:
            # Update ratings for a draw
            ratings = update_elo(winners, losers, ratings, draw=True)
        else:
            # Update ratings using the ELO formula
            ratings = update_elo(winners, losers, ratings)

    # Save the updated ratings back to HDFS
    output_file = "hdfs:///user/s2163918/output/updated_ratings.parquet"
    ratings_list = [(player, elo) for player, elo in ratings.items()]  # Convert dictionary to list of tuples
    ratings_schema = ["Player_ID", "ELO"]  # Define schema for the output DataFrame

    # Create a DataFrame from the updated ratings
    ratings_df = spark.createDataFrame(ratings_list, schema=ratings_schema)

    # Write the DataFrame to HDFS as a Parquet file
    ratings_df.write.parquet(output_file, mode="overwrite")

    print(f"Updated ratings saved to {output_file}")

# Command to run the script:
# spark-submit hdfs:///user/s2163918/input/ChronELO.py hdfs:///user/s2163918/input/games.parquet hdfs:///user/s2163918/input/ratings.parquet

# Input Files:
# 1. Games File (games.parquet): Format: Game_ID, Winners, Losers, Draw
#    Example: {"Game_ID": "1", "Winners": ["player1", "player2"], "Losers": ["player3", "player4"], "Draw": 0}
# 2. Ratings File (ratings.parquet): Format: Player_ID, ELO
