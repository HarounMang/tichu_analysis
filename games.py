from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, cast, row_number, first, countDistinct, size, collect_set, dense_rank, collect_list

# input: rows.csv: player,grand-tichu-cards,start-cards,deal-left,deal-middle,deal-right,gr-tichu,out,wish,tichu,game-id,score,bomb-received
# output: games.csv: game-id, [winner-id1, winner-id2], [loser-id1, loser-id2], draw

spark = SparkSession.builder.getOrCreate()

#Dataframe
windowSpec  = Window.partitionBy('game-id').orderBy(col('total_score').desc())
windowSpec2 = Window.partitionBy('game-id')

# read parquet file
# compute total score per game-id per player, and store (game-id, player, total_score)
# create row order sorted on descending total_score
# count number of distinct scores, mod2, indicates draw (1 if yes, 0 if no)
# group per game-id into a row with a winner-array and a loser-array
# concatenate the winners and losers into 1 row per game-id
# rename and move
# write to csv
games =  spark.read.parquet("parquet_df")\
    .groupBy('game-id', 'player').agg(sum('score').cast("int").alias('total_score'))\
    .withColumn("row_number",dense_rank().over(windowSpec))\
    .withColumn('draw', size(collect_set('total_score').over(windowSpec2))%2)\
    .groupBy("game-id", 'row_number', 'draw').agg(collect_list("player").alias("players"))\
    .groupBy("game-id", 'draw').pivot("row_number", [1, 2]).agg(first("players"))\
    .select(col("game-id"), col("1").alias("winners"), col("2").alias("losers"), col('draw'))\
    .write.parquet("games", mode="overwrite")

''''
games = spark.read.csv("rows.csv", header = True)\
    .select(col('game-id'), col('player'), col('score').cast("int"))\
    .rdd\
    .map(lambda t: ((t[0],t[1]),t[2]))\        # (game-id, player), score)
    .reduceByKey(lambda a, b: a+b)\            # compute total score per game-id and player combination
    .sortBy(lambda t: t[1], ascending=False)\  # sort by total score
    .map(lambda t: (t[0][0], t[0][1]))         # only keep (game-id, player)
    .groupByKey()\                             # concatenate players in order DOES NOT KEEP ORDER
    .map(lambda x: tuple([x[0]] + list(x[1]))) # make it into a list

csv_file = GAMES.map(lambda row: ",".join(row))
spark.sparkContext.parallelize([header]).union(games).saveAsTextFile("games.csv")
'''