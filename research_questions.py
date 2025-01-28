# This file executes the queries to answer our research questions

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, array_intersect, lit, size, concat, explode, array_contains

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Calculate the percentage of hands that won after calling (Grand) Tichu
def wins_with_tichu_call(df, call, hand):
    # Filter the rows which called (Grand) Tichu
    filtered = df.filter(col(call) == 1)

    # # Filter the rows which were out first
    # print("\nAll hands")
    # win_percentage(filtered)

    # # Check the percentage of wins with the Dragon and the Phoenix
    # cards = array(lit("Dr"), lit("Ph"))
    # hands = filtered.filter(size(array_intersect(col(hand), cards)) == 2)

    # print("\nThe hand contains a Dragon and a Phoenix")
    # win_percentage(hands)

    # # Check the percentage of wins with at least four of the best eight cards
    # cards = array(lit("Dr"), lit("Ph"), lit("Hu"), lit("Ma"), lit("RA"), lit("SA"), lit("GA"), lit("BA"))
    # hands = filtered.filter(size(array_intersect(col(hand), cards)) >= 4)

    # print("\nThe hand contains (at least) four high cards")
    # win_percentage(hands)

    # Check the percentage of wins with a bomb
    hands = filtered.filter(col('bomb') == 1)

    print("\nThe hand contains a bomb")
    win_percentage(hands)

# Calculate the percentage of hands that won
def win_percentage(hands):
    total = hands.count()
    wins = hands.filter(col('out') == 1.0).count()

    print(round(wins / total * 100, 1), "%     (", wins, "out of", total, ")")
    return round(wins / total * 100, 1) if total > 0 else 0

# Find the most common cards in a hand that had an accurate (Grand) Tichu call
def cards_with_winning_tichu_call(df, call, hand):
    # Filter the rows which called (Grand) Tichu and won
    filtered = df.filter((col(call) == 1) & (col('out') == 1.0))

    # Count the frequencies of cards in winning hands
    counts = (
        filtered
        .select(explode(col(hand)).alias('card'))
        .groupBy('card').count()
        .orderBy(col('count').desc()).limit(8)
        )
    
    # Print the eight most frequent cards
    print("\nThe eight most frequent cards are:\n", counts.show())

def passing_the_dog(df):
    # Filter the rows which called Grand Tichu
    filtered = df.filter(col('gr-tichu') == 1)

    # Find the percentage of wins with the Dog
    hands = filtered.filter(array_contains(col('gr-tichu-cards'), lit('Hu')))

    print("\nPlayers won with the dog in their Grand Tichu cards")
    win_percentage(hands)

    # Find the percentage of wins after passing the Dog to the ally
    hands = filtered.filter(col('deal-middle') == 'Hu')

    print("\nPlayers won after passing the dog to their ally")
    win_percentage(hands)

    # Find the percentage of wins after receiving the dog from an opponent    
    hands = filtered.filter((~array_contains(col('tichu-cards'), lit('Hu'))) & (array_contains(col('start-cards'), lit('Hu'))))

    print("\nPlayers won after receiving the dog from another player")
    win_percentage(hands)
    
def queries(df):
    calls = df.select(col('gr-tichu'), col('tichu'), col('gr-tichu-cards'), col('out'), col('extra-cards'), col('bomb'))

    print("\n- GRAND TICHU -")

    # Analyse the wins which called Grand Tichu
    wins_with_tichu_call(calls, 'gr-tichu', 'gr-tichu-cards')

    # Analyse which cards the player had when calling an accurate Grand Tichu
    cards_with_winning_tichu_call(calls, 'gr-tichu', 'gr-tichu-cards')

    print("\n- TICHU -")

    # Analyse the wins which called Tichu
    wins_with_tichu_call(calls, 'tichu', 'extra-cards')

    # Analyse which cards the player had when calling an accurate Tichu
    cards_with_winning_tichu_call(calls, 'tichu', 'extra-cards')

    print("\n- PASSING THE DOG -")

    calls = df.select(col('gr-tichu'), col('gr-tichu-cards'), col('out'), col('extra-cards'), col('start-cards'), col('deal-middle')).withColumn(
        'tichu-cards',
        concat(col('gr-tichu-cards'), col('extra-cards'))
    )
    
    # Analyse the effect of the dog on Grand Tichu
    passing_the_dog(calls)

if __name__ == "__main__":
    # Initialize Spark session for distributed processing
    spark = SparkSession.builder.appName("RQs").getOrCreate()

    # Path to the parquet file in HDFS
    file_data = "hdfs:///user/s2829541/final_tichu_data" # "hdfs:///user/s2163918/input/rows.csv"
    elo_path = "hdfs:///user/s2163918/elo_results"

    # Load the file into a DataFrame
    df = spark.read.parquet(file_data)
    elo_df = spark.read.parquet(elo_path)

    joined_df = df.join(elo_df, df.player == elo_df.Player_ID).drop('Player_ID')\
        .withColumn('bomb', col('bomb-received').cast("int"))

    print("\n\n--- LOW ELO ---")
    queries(joined_df.where(col('ELO') <= 1200))

    print("\n\n--- HIGH ELO ---")
    queries(joined_df.where(col('ELO') >= 1800))

    print("\n\n--- ALL ---")
    queries(df)
