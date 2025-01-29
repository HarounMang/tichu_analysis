from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, array_union, udf, col, sum, cast, row_number, first, countDistinct, size, collect_set, dense_rank, collect_list
from pyspark.sql.types import IntegerType, BooleanType
from pyspark.sql.window import Window

# input: game-id, round, player-id, player, gr-tichu-cards,extra-cards,deal-left,deal-middle,deal-right,start-cards,gr-tichu,out,wish,tichu,score,bomb-received
# output:game-id, [winner-id1, winner-id2], [loser-id1, loser-id2], draw

spark = SparkSession.builder.getOrCreate()
windowSpec  = Window.partitionBy('game-id', 'round').orderBy(col('score').desc())
windowSpec2 = Window.partitionBy('game-id', 'round')

#thresholds for high and low elo
high_elo = 1574
low_elo = 1437

special_cards = ['Ma', 'Hu', 'Ph', 'Dr']
contains_special_card = udf(lambda x: x in special_cards, BooleanType())

# read parquet file of data
# read parquet file of ELO
# rename column of ELO
# join both dataframes
df =  spark.read.parquet('/user/s2829541/final_tichu_data')\
    .select('player', 'game-id', 'round', 'gr-tichu-cards', 'extra-cards', 'deal-left', 'deal-right', 'score')\
    .withColumn('rank',dense_rank().over(windowSpec))
ELO = spark.read.parquet('/user/s2163918/elo_results_test').select('player_id', 'ELO')\
    .withColumn('player', col('player_id')).drop('player_id')
df = df.join(ELO, on='player', how='inner')

# filter on first 14 cards containing any special cards
hands_with_special_cards = df.filter(array_contains(array_union(df['gr-tichu-cards'], df['extra-cards']), 'Ma')|\
            array_contains(array_union(df['gr-tichu-cards'], df['extra-cards']), 'Hu')|\
            array_contains(array_union(df['gr-tichu-cards'], df['extra-cards']), 'Ph')|\
            array_contains(array_union(df['gr-tichu-cards'], df['extra-cards']), 'Dr'))

num_deal_special_cards_to_opponent = hands_with_special_cards.filter(contains_special_card('deal-left') | contains_special_card('deal-right')).count()

high_elo_with_special_cards = hands_with_special_cards.filter(hands_with_special_cards['ELO']>=high_elo)
low_elo_with_special_cards = hands_with_special_cards.filter(hands_with_special_cards['ELO']<=low_elo)
num_hands_with_special_cards_high_elo = high_elo_with_special_cards.count()
num_hands_with_special_cards_low_elo = low_elo_with_special_cards.count()
print('number of hands with high elo that contain a special card:', num_hands_with_special_cards_high_elo)
print('number of hands with low elo that contain a special card:', num_hands_with_special_cards_low_elo)

num_high_elo_deal_to_opponent = high_elo_with_special_cards.filter(contains_special_card('deal-left') | contains_special_card('deal-right')).count()
num_low_elo_deal_to_opponent = low_elo_with_special_cards.filter(contains_special_card('deal-left') | contains_special_card('deal-right')).count()
print('number of hands with high elo that play special card card to opponent:', num_high_elo_deal_to_opponent)
print('number of hands with low elo that play special card card to opponent:', num_low_elo_deal_to_opponent)

print('- - - -')

print('percentage of high elo players that play special card to opponent:', num_high_elo_deal_to_opponent/num_hands_with_special_cards_high_elo*100, '%')
print('percentage of low elo players that play special card to opponent:', num_low_elo_deal_to_opponent/num_hands_with_special_cards_low_elo*100, '%')

print('- - - -')
# only keep wins
df_wins_special_cards_in_hand = hands_with_special_cards.withColumn('distinct', size(collect_set('score').over(windowSpec2))).filter(col('distinct') == 2).drop('distinct')\
    .filter(col('rank')==1)\

df_wins_special_card_to_opponent = df_wins_special_cards_in_hand.filter(contains_special_card('deal-left') | contains_special_card('deal-right'))

print('percentage of winning, when playing special card to opponent:', df_wins_special_card_to_opponent.count()/num_deal_special_cards_to_opponent*100, '%')
print('percentage of winning, when not playing special card to opponent:', (df_wins_special_cards_in_hand.count()-df_wins_special_card_to_opponent.count())/(hands_with_special_cards.count()-num_deal_special_cards_to_opponent)*100, '%')

print('- - - -')

print('high elo - percentage of winning, when playing special card to opponent:', df_wins_special_card_to_opponent.filter(col('ELO')>=high_elo).count()/num_high_elo_deal_to_opponent*100 ,'%')
print('high elo - percentage of winning, when not playing special card to opponent:', (df_wins_special_cards_in_hand.filter(col('ELO')>=high_elo).count()-df_wins_special_card_to_opponent.filter(col('ELO')>=high_elo).count())/(num_hands_with_special_cards_high_elo-num_high_elo_deal_to_opponent) *100, '%')