# This file executes the queries to answer our research questions

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_intersect, size, concat, explode

# Calculate the percentage of hands that won after calling (Grand) Tichu
def wins_with_tichu_call(df, call, hand):
    # Calculate the percentage of hands that won
    def win_percentage(hands):
        total = hands.count()
        wins = hands.filter(col('out') == 1.0).count()
        return wins/total if total > 0 else 0

    # Filter the rows which called (Grand) Tichu
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

    # Check the percentage of wins with a bomb
    hands = filtered.filter(col('bomb-received') == 1)
    
    wins = win_percentage(hands)
    print(wins, "% of players won after their call with a bomb")

# Find the most common cards in a hand that had an accurate (Grand) Tichu call
def cards_with_winning_tichu_call(df, call, hand):
    # Filter the rows which called (Grand) Tichu and won
    filtered = df.filter(col(call) == 1 and col('out') == 1)

    # Count the frequencies of cards in winning hands
    counts = (
        filtered
        .select(explode(col(hand)).alias('card'))
        .groupBy('card').count()
        .orderBy(col('count').desc()).limit(8)
        )
    
    # Print the eight most frequent cards
    print("\nThe eight most frequent cards are:\n", counts)
    
if __name__ == "__main__":
    # Initialize Spark session for distributed processing
    spark = SparkSession.builder.appName("RQs").getOrCreate()

    # Path to the rows.csv file in HDFS
    csv = "hdfs:///user/s2163918/input/rows.csv"

    # Load the csv into a DataFrame
    df = spark.read.csv(csv, header=True)
    
    calls = df.select(col('gr-tichu-cards'), col('out'), col('remaining-cards'), col('bomb-received')).withColumn(
        'tichu-cards',
        concat(col('gr-tichu-cards'), col('remaining-cards'))
    )

    print("\n\n--- GRAND TICHU ---")

    # Analyse the wins which called Grand Tichu
    wins_with_tichu_call(calls, 'gr-tichu', 'gr-tichu-cards')

    # Analyse which cards the player had when calling an accurate Grand Tichu
    cards_with_winning_tichu_call(calls, 'gr-tichu', 'gr-tichu-cards')

    print("\n\n--- TICHU ---")

    # Analyse the wins which called Tichu
    wins_with_tichu_call(calls, 'tichu', 'tichu-cards')

    # Analyse which cards the player had when calling an accurate Tichu
    cards_with_winning_tichu_call(calls, 'tichu', 'tichu-cards')
