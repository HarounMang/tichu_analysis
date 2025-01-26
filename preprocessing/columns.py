from enum import Enum


class Type(Enum):
    INTEGER = "integer"
    STRING = "string"
    FLOAT = "float"
    STRING_ARRAY = "string_array"

COLUMNS = {
    "game-id": {
        "type": Type.STRING,
        "nullable": False,
    },
    "round": {
        "type": Type.INTEGER,
        "nullable": False,
    },
    "player_id": {
        "type": Type.INTEGER,
        "nullable": False,
    },
    "player": {
        "type": Type.STRING,
        "nullable": False,
    },
    "gr-tichu-cards": {
        "type": Type.STRING_ARRAY,
        "nullable": False,
    },
    "extra-cards": {
        "type": Type.STRING_ARRAY,
        "nullable": False
    },
    "deal-left": {
        "type": Type.STRING,
        "nullable": False
    },
    "deal-middle": {
        "type": Type.STRING,
        "nullable": False
    },
    "deal-right": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-cards": {
        "type": Type.STRING_ARRAY,
        "nullable": False
    },
    "gr-tichu": {
        "type": Type.INTEGER,
        "nullable": False
    },
    "out": {
        "type": Type.FLOAT,
        "nullable": False
    },
    "wish": {
        "type": Type.STRING,
        "nullable": True
    },
    "tichu": {
        "type": Type.INTEGER,
        "nullable": False
    },
    "score": {
        "type": Type.INTEGER,
        "nullable": False
    },
    "bomb-received": {
        "type": Type.INTEGER,
        "nullable": False
    },
}