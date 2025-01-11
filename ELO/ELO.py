from pyspark.sql import SparkSession
import sys


# Function to calculate the expected score for a team based on Elo ratings
def calculate_expected_score(team_rating, opponent_team_rating):
    # Elo formula: Expected score = 1 / (1 + 10^((OpponentRating - TeamRating) / 400))
    # This formula determines the probability of a team winning against another team
    return 1 / (1 + 10 ** ((opponent_team_rating - team_rating) / 400))


# Function to update the Elo ratings after a match
def update_elo(winner_team, loser_team, ratings, k_factor=32):
    # Calculate the average Elo rating for the winner team
    winner_team_rating = sum(ratings.get(player, 1500) for player in winner_team) / len(winner_team)

    # Calculate the average Elo rating for the loser team
    loser_team_rating = sum(ratings.get(player, 1500) for player in loser_team) / len(loser_team)

    # Calculate the expected scores for both teams
    expected_winner = calculate_expected_score(winner_team_rating, loser_team_rating)
    expected_loser = 1 - expected_winner  # Complement of the winner's expected score

    # Update Elo ratings for players in the winner team
    for player in winner_team:
        ratings[player] = ratings.get(player, 1500) + k_factor * (1 - expected_winner)
        # Elo update: Rating = CurrentRating + K * (ActualScore - ExpectedScore)
        # ActualScore for winner = 1

    # Update Elo ratings for players in the loser team
    for player in loser_team:
        ratings[player] = ratings.get(player, 1500) + k_factor * (0 - expected_loser)
        # ActualScore for loser = 0

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
        winner_team = row["Winner_Team"].split(';')
        loser_team = row["Loser_Team"].split(';')

        # Update ratings using the Elo formula
        ratings = update_elo(winner_team, loser_team, ratings)

    # Save the updated ratings back to HDFS
    output_file = "hdfs:///user/s2163918/output/updated_ratings.csv"
    ratings_list = [(player, elo) for player, elo in ratings.items()]  # Convert dictionary to list of tuples
    ratings_schema = ["Player_ID", "ELO"]  # Define schema for the output DataFrame

    # Create a DataFrame from the updated ratings
    ratings_df = spark.createDataFrame(ratings_list, schema=ratings_schema)

    # Write the DataFrame to HDFS as a single CSV file, with headers
    ratings_df.coalesce(1).write.csv(output_file, header=True, mode="overwrite")

    print(f"Updated ratings saved to {output_file}")

# How the Elo formula works:
# - Each player/team has an Elo rating that represents their skill level.
# - The expected score is calculated based on the ratings of the two teams.
# - Ratings are updated after each match using the formula:
#     NewRating = CurrentRating + K * (ActualScore - ExpectedScore)
#     - K is a constant that determines the sensitivity of rating changes.
#     - ActualScore is 1 for a win and 0 for a loss.
#     - ExpectedScore is the predicted outcome based on the Elo formula.
# - Over time, stronger players/teams will have higher ratings as they win more often than expected.

# Command to run the script:
# spark-submit ELO.py hdfs:///user/s2163918/input/games.csv hdfs:///user/s2163918/input/ratings.csv

# Input Files:
# 1. Games File (games.csv): Format: Game_ID,Winner_Team,Loser_Team
#    Example: 1,player1;player2,player3;player4
# 2. Ratings File (ratings.csv): Format: Player_ID,ELO
#    Example: player1,1500
