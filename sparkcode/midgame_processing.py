from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType

from enum import Enum

CORRUPT = "corrupt"

def csv_rows(rnd: str, rnd_id: int) -> list[any]:
    head_other_split = rnd.split("---------------Rundenverlauf------------------\n", 1)
    if len(head_other_split) < 2:
        return [[CORRUPT]]
    body = head_other_split[1]
    turns = body.strip().split("\n")[:-1]

    turn = 0
    tichu_called = False
    dragon_given_to = None
    last_played_turn_before_dragon_dealt = None

    rows: list[list] = []

    for i, line in enumerate(turns):
        if line[:7] == "Tichu: ":
            tichu_called = True
            continue

        if line[:7] == "Wunsch:":
            continue

        # dragon given
        if line[:11] == "Drache an: ":
            dragon_given_to = line.split(": ", 1)[1][3:]
            last_played_turn_before_dragon_dealt = None
            continue

        splitted_line = line.split(": ", 1)
        player = splitted_line[0][3:].split(" passt.", 1)[0]
        player_id = int(line[1])

        turn += 1

        if line[-6:] == "passt.":
            rows.append([rnd_id, turn, player_id, player, None, int(tichu_called)])
            continue

        cards = splitted_line[1].strip().split(" ")

        # if last_played_turn_before_dragon_dealt is not None:
        #     last_played_turn_before_dragon_dealt = turn

        if cards[0] == "Dr":
            last_played_turn_before_dragon_dealt = turn

        rows.append([rnd_id, turn, player_id, player, cards, int(tichu_called)])

        tichu_called = False    

    for row in rows:
        if row[1] == last_played_turn_before_dragon_dealt:
            row.append(dragon_given_to)
        else:
            row.append(None)

    return rows


def process_text_file(text_file: str, game_id: str) -> list:
    rounds = text_file.split("---------------Gr.Tichukarten------------------\n")[1:]

    resulting_rows = []
    for i, rnd in enumerate(rounds):
        rnd_rows = csv_rows(rnd, i)  # i is round id
        for row in rnd_rows:
            resulting_rows.append([game_id] + row)
    return resulting_rows


class Type(Enum):
    INTEGER = "integer"
    STRING = "string"
    FLOAT = "float"
    STRING_ARRAY = "string_array"

COLUMNS = {
    "game-id": {
        "type": Type.INTEGER,
        "nullable": False,
    },
    "round": {
        "type": Type.INTEGER,
        "nullable": False,
    },
    "turn": {
        "type": Type.INTEGER,
        "nullable": False
    },
    "player-id": {
        "type": Type.INTEGER,
        "nullable": False,
    },
    "player": {
        "type": Type.STRING,
        "nullable": False,
    },
    "cards": {
        "type": Type.STRING_ARRAY,
        "nullable": True,
    },
    "tichu_called": {
        "type": Type.INTEGER,
        "nullable": True,
    },
    "dragon_passed_to": {
        "type": Type.STRING,
        "nullable": True,
    }
}

spark_sql_type = {
    Type.STRING: StringType(),
    Type.INTEGER: IntegerType(),
    Type.FLOAT: FloatType(),
    Type.STRING_ARRAY: ArrayType(StringType(), containsNull=False)
}

def map_to_rows(rdd_entry: tuple[str, str]):
    game_id = int(rdd_entry[0].split("/")[-1].split(".")[0])
    last_line = rdd_entry[1].strip().split("\n")[-1]
    if "Ergebnis" not in last_line:
        return [[game_id, CORRUPT]] 
    return process_text_file(rdd_entry[1], game_id)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .getOrCreate()
    sc = spark.sparkContext

    rdd = sc.wholeTextFiles('/user/s2860406/dev_split')
    processed_rdd = rdd.flatMap(map_to_rows)
    processed_rdd = processed_rdd.filter(lambda x: len(x) == len(COLUMNS))
    
    schema = StructType([
        StructField(key, spark_sql_type[value["type"]], value["nullable"]) for key, value in COLUMNS.items()
    ])

    df = spark.createDataFrame(processed_rdd, schema=schema)
    
    from pyspark.sql.functions import col
    df.where(col('dragon_passed_to') == "Punica").show()
    # PAS DE FOLDER AAN NA IEDERE SPLIT
    df.write.parquet("/user/s2829541/midgame_data", mode="overwrite")
