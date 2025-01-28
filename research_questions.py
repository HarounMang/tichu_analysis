# This file executes the queries to answer our research questions

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, array_intersect, lit, size, concat, explode, array_contains

import os
import sys

# Calculate the percentage of hands that won after calling (Grand) Tichu
def wins_with_tichu_call(df, hand):
    # Filter the rows which were out first
    print("All hands")
    win_percentage(df)

    # Check the percentage of wins with the Dragon and the Phoenix
    cards = array(lit("Dr"), lit("Ph"))
    hands = df.filter(size(array_intersect(col(hand), cards)) == 2)

    print("Wins if the hand contains a Dragon and a Phoenix")
    win_percentage(hands)

    # Check the percentage of wins with at least four of the best eight cards
    cards = array(lit("Dr"), lit("Ph"), lit("Hu"), lit("Ma"), lit("RA"), lit("SA"), lit("GA"), lit("BA"))
    hands = df.filter(size(array_intersect(col(hand), cards)) >= 4)

    print("Wins if the hand contains (at least) four high cards (Dog and Mahjong)")
    win_percentage(hands)

    # Check the percentage of wins with at least four of the best ten cards
    cards = array(lit("Dr"), lit("Ph"), lit("RA"), lit("SA"), lit("GA"), lit("BA"), lit("RK"), lit("SK"), lit("GK"), lit("BK"))
    hands = df.filter(size(array_intersect(col(hand), cards)) >= 4)

    print("Wins if the hand contains (at least) four high cards (all kings)")
    win_percentage(hands)

    # Check the percentage of wins with at least four of the best ten cards
    cards = array(lit("Dr"), lit("Ph"), lit("RA"), lit("SA"), lit("GA"), lit("BA"), lit("RK"), lit("SK"), lit("GK"), lit("BK"))
    kings = array(lit("RK"), lit("SK"), lit("GK"), lit("BK"))
    hands = df.filter((size(array_intersect(col(hand), cards)) >= 4) & (size(array_intersect(col(hand), kings)) <= 2))

    print("Wins if the hand contains (at least) four high cards (two kings)")
    win_percentage(hands)

    # Check the percentage of wins with a bomb
    hands = df.filter(col('bomb-received') == 1)

    print("Wins if the hand contains a bomb")
    win_percentage(hands)

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
    print("\The eight most frequent cards are:")
    counts.show()

# Find the most common cards in a hand that had an accurate (Grand) Tichu call
def cards_with_winning_hand(df):
    # Filter the rows which won
    filtered = df.filter(col('out') == 1.0)

    # Count the frequencies of cards in winning hands
    counts = (
        filtered
        .select(explode(col('start-cards')).alias('card'))
        .groupBy('card').count()
        .orderBy(col('count').desc()).limit(8)
        )
    
    # Print the eight most frequent cards
    print("The eight most frequent cards are:")
    counts.show()

# Find the most used strategy with regards to the dog
def passing_the_dog(df):
    # Filter the rows which called Grand Tichu
    filtered = df.filter(col('gr-tichu') == 1)

    # Find the percentage of wins with the Dog
    hands = filtered.filter(array_contains(col('gr-tichu-cards'), lit('Hu')))

    print("Wins with the dog in their Grand Tichu cards")
    win_percentage(hands)

    # Find the percentage of wins with the Dog
    hands = filtered.filter(array_contains(col('tichu-cards'), lit('Hu')))

    print("Wins with the dog in their Tichu cards")
    win_percentage(hands)

    # Find the percentage of wins after passing the Dog to the ally
    hands = filtered.filter(col('deal-middle') == 'Hu')

    print("Wins after passing the dog to their ally")
    win_percentage(hands)

    # Find the percentage of wins after receiving the dog from an opponent    
    hands = filtered.filter((~array_contains(col('tichu-cards'), lit('Hu'))) & (array_contains(col('start-cards'), lit('Hu'))))

    print("Wins after receiving the dog from another player")
    win_percentage(hands)

# Finding which strategies work and which do not
def strategies(df, call, hand):
    # The number of calls with the Dragon and the Phoenix
    cards = array(lit("Dr"), lit("Ph"))
    hands = df.filter(size(array_intersect(col(hand), cards)) == 2)

    print("The number of calls with a Dragon and a Phoenix")
    win_percentage(hands)

    # The number of calls with (at least) four of the best eight cards
    cards = array(lit("Dr"), lit("Ph"), lit("Hu"), lit("Ma"), lit("RA"), lit("SA"), lit("GA"), lit("BA"))
    hands = df.filter(size(array_intersect(col(hand), cards)) >= 4)

    print("The number of calls with (at least) four high cards (Dog and Mahjong)")
    call_percentage(hands, call)

    # Check the percentage of wins with at least four of the best ten cards
    cards = array(lit("Dr"), lit("Ph"), lit("RA"), lit("SA"), lit("GA"), lit("BA"), lit("RK"), lit("SK"), lit("GK"), lit("BK"))
    hands = df.filter(size(array_intersect(col(hand), cards)) >= 4)

    print("Wins if the hand contains (at least) four high cards (all kings)")
    call_percentage(hands, call)

    # Check the percentage of wins with at least four of the best ten cards
    cards = array(lit("Dr"), lit("Ph"), lit("RA"), lit("SA"), lit("GA"), lit("BA"), lit("RK"), lit("SK"), lit("GK"), lit("BK"))
    kings = array(lit("RK"), lit("SK"), lit("GK"), lit("BK"))
    hands = df.filter((size(array_intersect(col(hand), cards)) >= 4) & (size(array_intersect(col(hand), kings)) <= 2))

    print("Wins if the hand contains (at least) four high cards (two kings)")
    call_percentage(hands, call)

    # The number of calls with (at least) four of the best eight cards
    hands = df.filter(col('bomb-received') == 1)

    print("The number of calls with a bomb")
    call_percentage(hands, call)

# Calculate the percentage of hands that won
def win_percentage(hands):
    total = hands.count()
    wins = hands.filter(col('out') == 1.0).count()

    print(round(wins / total * 100, 1), "%     (", wins, "out of", total, ")\n")
    return round(wins / total * 100, 1) if total > 0 else 0

# Calculate the percentage of hands that won
def call_percentage(hands, call):
    total = hands.count()
    calls = hands.filter(col(call) == 1).count()

    print(round(calls / total * 100, 1), "%     (", calls, "out of", total, ")\n")
    return round(calls / total * 100, 1) if total > 0 else 0

def analysing_cards(df):  
    print("- GRAND TICHU -")

    calls = df.select(col('gr-tichu'), col('gr-tichu-cards'), col('out'), col('bomb-received'))

    # Analyse which cards the player had when calling an accurate Grand Tichu
    cards_with_winning_tichu_call('gr-tichu', 'gr-tichu-cards')

    print("\n- TICHU -")

    calls = df.select(col('tichu'), col('gr-tichu'), col('start-cards'), col('out'), col('bomb-received'))\
        .withColumn('both-tichu', (col('gr-tichu').cast("boolean") | col('tichu').cast("boolean")).cast("int")).drop('gr-tichu')

    # Analyse which cards the player had when calling an accurate Tichu
    cards_with_winning_tichu_call(calls, 'tichu', 'start-cards')

    print("\n- ALL -")

    # Analyse which cards the player had when they won a round
    cards_with_winning_hand(calls)

def queries(df):
    print("- GRAND TICHU -")
    calls = df.select(col('gr-tichu'), col('gr-tichu-cards'), col('out'), col('bomb-received'))

    # Analyse the wins which called Grand Tichu
    wins_with_tichu_call(df.filter(col('gr-tichu') == 1), 'gr-tichu-cards')

    # Analyse which strategies work well
    strategies(calls, 'gr-tichu', 'gr-tichu-cards')

    print("\n- TICHU -")
    calls = df.select(col('tichu'), col('gr-tichu'), col('start-cards'), col('out'), col('bomb-received'))\
        .withColumn('both-tichu', (col('gr-tichu').cast("boolean") | col('tichu').cast("boolean")).cast("int")).drop('gr-tichu')

    # Analyse the wins which called Tichu
    wins_with_tichu_call(df.filter(col('tichu') == 1), 'start-cards')

    # Analyse which strategies work well
    strategies(calls, 'both-tichu', 'start-cards')

    print("\n - ALL HANDS -")
    # Analyse the wins which called Grand Tichu
    wins_with_tichu_call(df, 'gr-tichu-cards')

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
    elo_path = "hdfs:///user/s2163918/elo_results_test"

    # Load the file into a DataFrame
    df = spark.read.parquet(file_data)
    elo_df = spark.read.parquet(elo_path)

    joined_df = df.join(elo_df, df.player == elo_df.Player_ID).drop('Player_ID')

    print("--- LOW ELO ---")
    queries(joined_df.where(col('ELO') <= 1437))

    print("\n\n--- HIGH ELO ---")
    queries(joined_df.where(col('ELO') >= 1574))

    print("\n\n--- ALL ---")
    queries(df)

    analysing_cards(df)

    
