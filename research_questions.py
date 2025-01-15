# This file executes the queries to answer our research questions

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_intersect, size

def wins_with_tichu(df, call, hand):
    # Filter the rows which called Grand Tichu
    filtered = df.filter(col(call) == 1)

    # Filter the rows which were out first
    total = filtered.count()
    wins = filtered.filter(df.out == 1.0).count()/total if total > 0 else 0
    print(wins, "% of players won after calling Grand Tichu")

    # Check the percentage of wins with the Dragon and the Phoenix
    cards = ["Dr", "Ph"]
    hands = filtered.filter(size(array_intersect(col(hand), cards)) == 2)
    total = hands.count()
    wins = hands.filter(df.out == 1.0).count()/total if total > 0 else 0
    print(wins, "% of players won after calling Grand Tichu with a Dragon and a Phoenix")

    # Check the percentage of wins with at least four of the best eight cards
    cards = ["Dr", "Ph", "Hu", "Ma", "RA", "SA", "GA", "BA"]
    hands = filtered.filter(size(array_intersect(col(hand), cards)) >= 4).count()
    total = hands.count()
    wins = hands.filter(df.out == 1.0).count()/total if total > 0 else 0
    print(wins, "% of players won after calling Grand Tichu with four high cards")

if __name__ == "__main__":
    # Initialize Spark session for distributed processing
    spark = SparkSession.builder.appName("ELO").getOrCreate()

    # Path to the rows.csv file in HDFS
    csv = "hdfs:///user/s2163918/input/rows.csv"

    # Load the csv into a DataFrame
    df = spark.read.csv(csv, header=True)

    # Analyse the wins which called Grand Tichu
    wins_with_tichu(df, 'gr-tichu', 'gr-tichu-cards')

    # Analyse the wins which called Tichu
    wins_with_tichu(df, 'tichu', 'remaining-cards')
