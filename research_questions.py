# This file executes the queries to answer our research questions

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, array_intersect, lit, size, concat, explode, array_contains

# Calculate the percentage of hands that won after calling (Grand) Tichu
def wins_with_tichu_call(df, call, hand):
    # Filter the rows which called (Grand) Tichu
    filtered = df.filter(col(call) == 1)

    # Filter the rows which were out first
    wins = win_percentage(filtered)
    print(wins, "% of players won after their call")

    # Check the percentage of wins with the Dragon and the Phoenix
    cards = array(lit("Dr"), lit("Ph"))
    hands = filtered.filter(size(array_intersect(col(hand), cards)) == 2)

    wins = win_percentage(hands)
    print(wins, "% of players won after their call with a Dragon and a Phoenix")

    # Check the percentage of wins with at least four of the best eight cards
    cards = array(lit("Dr"), lit("Ph"), lit("Hu"), lit("Ma"), lit("RA"), lit("SA"), lit("GA"), lit("BA"))
    hands = filtered.filter(size(array_intersect(col(hand), cards)) >= 4)

    wins = win_percentage(hands)
    print(wins, "% of players won after their call with (at least) four high cards")

    # Check the percentage of wins with a bomb
    hands = filtered.filter(col('bomb-received') == 1)
    
    wins = win_percentage(hands)
    print(wins, "% of players won after their call with a bomb")

# Calculate the percentage of hands that won
def win_percentage(hands):
    total = hands.count()
    wins = hands.filter(col('out') == 1.0).count()
    print("\n", wins, "/", total)
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
    print("\nThe eight most frequent cards are:\n", counts.collect())

def passing_the_dog(df):
    # Filter the rows which called Grand Tichu
    filtered = df.filter(col('gr-tichu') == 1)
    print(filtered.count())

    # Find the percentage of wins with the Dog
    hands = filtered.filter(array_contains(col('gr-tichu-cards'), lit('Hu')))

    wins = win_percentage(hands)
    print(wins, "% of players won after their call with the dog in their Grand Tichu cards")

    # Find the percentage of wins after passing the Dog to the ally
    hands = filtered.filter(col('deal-middle') == 'Hu')

    wins = win_percentage(hands)
    print(wins, "% of players won after passing the dog from their Grand Tichu cards")

    # Find the percentage of wins after receiving the dog from an opponent    
    hands = filtered.filter((~array_contains(col('tichu-cards'), lit('Hu'))) & (array_contains(col('start-cards'), lit('Hu'))))

    wins = win_percentage(hands)
    print(wins, "% of players won after receiving the dog from an opponent")
    
if __name__ == "__main__":
    # Initialize Spark session for distributed processing
    spark = SparkSession.builder.appName("RQs").getOrCreate()

    # Path to the parquet file in HDFS
    file = "hdfs:///user/s2829541/tichu_data_2" # "hdfs:///user/s2163918/input/rows.csv"

    # Load the file into a DataFrame
    df = spark.read.parquet(file, header=True)
    
    calls = df.select(col('gr-tichu'), col('tichu'), col('gr-tichu-cards'), col('out'), col('extra-cards'), col('bomb-received'))

    print("\n\n--- GRAND TICHU ---")

    # Analyse the wins which called Grand Tichu
    wins_with_tichu_call(calls, 'gr-tichu', 'gr-tichu-cards')

    # Analyse which cards the player had when calling an accurate Grand Tichu
    cards_with_winning_tichu_call(calls, 'gr-tichu', 'gr-tichu-cards')

    print("\n\n--- TICHU ---")

    # Analyse the wins which called Tichu
    wins_with_tichu_call(calls, 'tichu', 'extra-cards')

    # Analyse which cards the player had when calling an accurate Tichu
    cards_with_winning_tichu_call(calls, 'tichu', 'extra-cards')

    print("\n\n--- PASSING THE DOG ---")

    calls = df.select(col('gr-tichu'), col('gr-tichu-cards'), col('out'), col('extra-cards'), col('start-cards'), col('deal-middle')).withColumn(
        'tichu-cards',
        concat(col('gr-tichu-cards'), col('extra-cards'))
    )
    
    # Analyse the effect of the dog on Grand Tichu
    passing_the_dog(calls)
