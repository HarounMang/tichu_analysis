# This file executes the queries to answer our research questions

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_intersect, size, concat

def wins_with_tichu_call(df, call, hand):
    # Calculate the percentage of hands that won
    def win_percentage(hands):
        total = hands.count()
        wins = hands.filter(col('out') == 1.0).count()
        return wins/total if total > 0 else 0

    # Filter the rows which called Grand Tichu
    filtered = df.filter(col(call) == 1)

    # Filter the rows which were out first
    wins = win_percentage(filtered)
    print(wins, "% of players won after their call")

    # Check the percentage of wins with the Dragon and the Phoenix
    cards = ["Dr", "Ph"]
    hands = filtered.filter(size(array_intersect(col(hand), cards)) == 2)

    wins = win_percentage(hands)
    print(wins, "% of players won after their call with a Dragon and a Phoenix")

    # Check the percentage of wins with at least four of the best eight cards
    cards = ["Dr", "Ph", "Hu", "Ma", "RA", "SA", "GA", "BA"]
    hands = filtered.filter(size(array_intersect(col(hand), cards)) >= 4)

    wins = win_percentage(hands)
    print(wins, "% of players won after their call with four high cards")

def cards_with_winning_tichu_call():
    pass

if __name__ == "__main__":
    # Initialize Spark session for distributed processing
    spark = SparkSession.builder.appName("ELO").getOrCreate()

    # Path to the rows.csv file in HDFS
    csv = "hdfs:///user/s2163918/input/rows.csv"

    # Load the csv into a DataFrame
    df = spark.read.csv(csv, header=True)
    
    calls = df.select(col('gr-tichu-cards'), col('out'), col('remaining-cards')).withColumn(
        'tichu-cards',
        concat(col('gr-tichu-cards'), col('remaining-cards'))
    )

    print("--- GRAND TICHU ---")

    # Analyse the wins which called Grand Tichu
    wins_with_tichu_call(calls, 'gr-tichu', 'gr-tichu-cards')

    # Analyse which cards the player had when calling an accurate Grand Tichu
    cards_with_winning_tichu_call(calls, 'gr-tichu', 'gr-tichu-cards')

    print("--- TICHU ---")
    
    # Analyse the wins which called Tichu
    wins_with_tichu_call(calls, 'tichu', 'tichu-cards')

    # Analyse which cards the player had when calling an accurate Tichu
    cards_with_winning_tichu_call(calls, 'tichu', 'tichu-cards')
