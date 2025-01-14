# The ELO formula:
# - Each player/team has an ELO rating that represents their skill level.
# - The expected score is calculated based on the ratings of the two teams.
# - Ratings are updated after each match using the formula:
#     NewRating = CurrentRating + K * (ActualScore - ExpectedScore)
#     - K is a constant that determines the sensitivity of rating changes.
#     - ActualScore is 1 for a win and 0 for a loss.
#     - ExpectedScore is the predicted outcome based on the ELO formula.
# - Over time, stronger players/teams will have higher ratings as they win more often than expected.

from pyspark.sql import SparkSession
import sys

# Calculate the expected score for a team based on ELO ratings
def calculate_expected_score(team_rating, opponent_rating):
    # ELO formula: Expected score = 1 / (1 + 10 ^ ((OpponentRating - TeamRating) / 400))
    return 1 / (1 + 10 ** ((opponent_rating - team_rating) / 400))

# Update the ELO ratings after a match
def update_elo(winners, losers, ratings, k_factor=32):
    # Calculate the average ELO rating for the teams
    winners_rating = sum(ratings.get(player, 1500) for player in winners) / len(winners)
    losers_rating = sum(ratings.get(player, 1500) for player in losers) / len(losers)

    # Calculate if it was expected that this team won based on the ELO scores
    expectation_winner = calculate_expected_score(winners_rating, losers_rating)

    # Update ELO ratings for the players
    # A high expectation leads to a small increase in score for the winner and a low decrease for the loser
    for player in winners:
        ratings[player] = ratings.get(player, 1500) + k_factor * (1 - expectation_winner)

    for player in losers:
        ratings[player] = ratings.get(player, 1500) + k_factor * (-1 + expectation_winner)

    return ratings


if __name__ == "__main__":
    # Initialize Spark session for distributed processing
    spark = SparkSession.builder.appName("ELO").getOrCreate()

    # Paths for input files in HDFS (command-line arguments)
    games_file = sys.argv[1]  # Path to the games.csv file in HDFS
    ratings_file = sys.argv[2]  # Path to the ratings.csv file in HDFS

    # Load player ratings from HDFS into a dictionary
    ratings_df = spark.read.csv(ratings_file, header=True)
    ratings = {row["Player_ID"]: float(row["ELO"]) for row in ratings_df.collect()}

    # Load game data from HDFS into a DataFrame
    games_df = spark.read.csv(games_file, header=True)

    # Process each game chronologically to update ratings
    for row in games_df.collect():
        game_id = row["Game_ID"]  # Game identifier

        # Split Winner_Team and Loser_Team into individual player IDs
        winners = row["Winner_Team"].split(';')
        losers = row["Loser_Team"].split(';')

        # Update ratings using the ELO formula
        ratings = update_elo(winners, losers, ratings)

    # Save the updated ratings back to HDFS
    output_file = "hdfs:///user/s2163918/output/updated_ratings.csv"
    ratings_list = [(player, elo) for player, elo in ratings.items()]  # Convert dictionary to list of tuples
    ratings_schema = ["Player_ID", "ELO"]  # Define schema for the output DataFrame

    # Create a DataFrame from the updated ratings
    ratings_df = spark.createDataFrame(ratings_list, schema=ratings_schema)

    # Write the DataFrame to HDFS as a single CSV file, with headers
    ratings_df.coalesce(1).write.csv(output_file, header=True, mode="overwrite")

    print(f"Updated ratings saved to {output_file}")

# Command to run the script:
# spark-submit ELO.py hdfs:///user/s2163918/input/games.csv hdfs:///user/s2163918/input/ratings.csv

# Input Files:
# 1. Games File (games.csv): Format: Game_ID,Winner_Team,Loser_Team
#    Example: 1,player1;player2,player3;player4
# 2. Ratings File (ratings.csv): Format: Player_ID,ELO
#    Example: player1,1500
