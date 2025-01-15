from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, cast, row_number, first, countDistinct, size

# input: rows.csv: player,gr-tichu-card-1,gr-tichu-card-2,gr-tichu-card-3,gr-tichu-card-4,gr-tichu-card-5,gr-tichu-card-6,gr-tichu-card-7,gr-tichu-card-8,start-card-1,start-card-2,start-card-3,start-card-4,start-card-5,start-card-6,deal-left,deal-middle,deal-right,gr-tichu,out,wish,tichu,game-id,score,bomb-received
# output: games.csv: game-id, winner-id1. winner-id2, loser-id1, loser-id2

#RDD
header = "game-id,winner1,winner2,loser1,loser2, draw"

#Dataframe

windowSpec  = Window.partitionBy('game-id').orderBy(col('total_score').desc())
windowSpec2 = Window.partitionBy('game-id')

games =  spark.read.csv("rows.csv", header = True)\
    .groupBy('game-id', 'player').agg(sum('score').cast("int").alias('total_score'))\   # compute total score per game-id per player, and store (game-id, player, total_score)
    .withColumn("row_number",row_number().over(windowSpec))\                            # create row order sorted on descending total_score
    .withColumn('draw', size(collect_set('total_score').over(windowSpec2))%2)\          # count number of distinct scores, mod2, indicates draw (1 if yes, 0 if no)
    .groupBy("game-id").pivot("row_number", [1, 2, 3, 4]).agg(first("player"))\         # group per game-id into a row with winners and losers
    .select(col("game-id"), col("1").alias("winner1"), col("2").alias("winner2"), col("3").alias("loser1"),col("4").alias("loser2"), col('draw')) # rename and move
    .write.csv("games.csv", header=True, mode="overwrite")                              # write to csv

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