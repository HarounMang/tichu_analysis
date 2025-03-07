from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType

from enum import Enum

CORRUPT = "corrupt"

def split_dealing_turn(head: str):
    gr_cards_lines_other_split = head.split("---------------Startkarten------------------\n", 1)
    gr_cards_lines_split = gr_cards_lines_other_split[0].split("\n")
    start_cards_lines_other_split = gr_cards_lines_other_split[1].split("Schupfen:\n", 1)
    start_cards_lines_split = start_cards_lines_other_split[0].split("\n")[:-1]  # [:-1] to get rid of the empty string
    deal_lines_split = start_cards_lines_other_split[1].split("\n")[:-1]  # [:-1] to get rid of the empty string

    if "BOMBE: (" in deal_lines_split[-1]:
        deal_lines_split = deal_lines_split[:4]

    return gr_cards_lines_split, start_cards_lines_split, deal_lines_split

def process_starting_hands(gr_cards_line: str, start_cards_line: str, deal_line: str, hands: list[set[str]],
                              player_id: int) -> None:
    player_gr_cards_split = gr_cards_line.split(" ", 1)

    gr_cards = set()

    for card in player_gr_cards_split[1].split(" ")[:-1]:
        gr_cards.add(card)
        hands[player_id].add(card)

    extra_cards = set()

    for card in start_cards_line.split(" ", 1)[1].split(" ")[:-1]:
        if card not in gr_cards:
            extra_cards.add(card)
            hands[player_id].add(card)

    for deal_line in deal_line.split("gibt: ", 1)[1:]:
        for i, deal_string in enumerate(deal_line.split(": ", 3)[1:], start=1):
            card = deal_string.split(" - ", maxsplit=1)[0]
            if card not in hands[player_id]:
                break
            hands[player_id].remove(card)
            hands[(player_id + i) % 4].add(card)


def csv_rows(rnd: str, rnd_id: int) -> list[any]:
    head_other_split = rnd.split("---------------Rundenverlauf------------------\n", 1)
    if len(head_other_split) < 2:
        return [[CORRUPT]]
    head = head_other_split[0]
    body = head_other_split[1]
    turns = body.strip().split("\n")[:-1]

    turn = 0
    tichu_called = False
    dragon_given_to = None
    dragon_played_turn = None

    rows: list[list] = []

    hands: list[set[str]] = [set(), set(), set(), set()]
    gr_cards_lines_split, start_cards_lines_split, deal_lines_split = split_dealing_turn(head)
    for id_ in range(4):
        process_starting_hands(
            gr_cards_lines_split[id_], start_cards_lines_split[id_], deal_lines_split[id_], hands, id_
        )

    for line in turns:
        row = [rnd_id]

        if line[:7] == "Tichu: ":
            tichu_called = True
            continue

        if line[:7] == "Wunsch:":
            continue

        # dragon given
        if line[:11] == "Drache an: ":
            dragon_given_to = line.split(": ", 1)[1][3:]
            continue

        splitted_line = line.split(": ", 1)
        player = splitted_line[0][3:].split(" passt.", 1)[0]
        player_id = int(line[1])

        row.append(turn)
        turn += 1
        row.append(player_id)
        row.append(player)
        row.append(list(hands[player_id]))

        if line[-6:] == "passt.":
            rows.append(row + [None, None, int(tichu_called)])
            continue

        cards_played = splitted_line[1].strip().split(" ")

        # in case someone plays after the dragon is played, we do not store the turn the dragon is played
        if dragon_given_to is None and dragon_played_turn is not None:
            dragon_played_turn = None

        corrupt_cards = []
        for card in cards_played:
            if card == "Dr":
                dragon_played_turn = turn
            
            hand = hands[player_id]

            if card in hand:
                hands[player_id].remove(card)
            else:
                corrupt_cards.append(card)

        rows.append(row + [corrupt_cards or None, cards_played, int(tichu_called)])

        tichu_called = False    

    for row in rows:
        if row[1] == dragon_played_turn:
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
        "nullable": False,
    },
    "corrupt-cards": {
        "type": Type.STRING_ARRAY,
        "nullable": True,
    },
    "cards-played": {
        "type": Type.STRING_ARRAY,
        "nullable": True,
    },
    "tichu-called": {
        "type": Type.INTEGER,
        "nullable": True,
    },
    "dragon-passed-to": {
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

    rdd = sc.wholeTextFiles('/user/s2860406/split_1')
    processed_rdd = rdd.flatMap(map_to_rows)
    processed_rdd = processed_rdd.filter(lambda x: len(x) == len(COLUMNS))
    
    schema = StructType([
        StructField(key, spark_sql_type[value["type"]], value["nullable"]) for key, value in COLUMNS.items()
    ])

    df = spark.createDataFrame(processed_rdd, schema=schema)
    
    df.show()
    # PAS DE FOLDER AAN NA IEDERE SPLIT
    df.write.parquet("/user/s2860406/midgame_data/split_1", mode="overwrite")
