from enum import Enum


class Type(Enum):
    INTEGER = "integer"
    STRING = "string"
    FLOAT = "float"

COLUMNS = {
    "game-id": {
        "type": Type.INTEGER,
        "nullable": False,
    },
    "round": {
        "type": Type.INTEGER,
        "nullable": False,
    },
    "player": {
        "type": Type.STRING,
        "nullable": False,
    },
    "gr-tichu-card-1": {
        "type": Type.STRING,
        "nullable": False,
    },
    "gr-tichu-card-2": {
        "type": Type.STRING,
        "nullable": False
    },
    "gr-tichu-card-3": {
        "type": Type.STRING,
        "nullable": False
    },
    "gr-tichu-card-4": {
        "type": Type.STRING,
        "nullable": False
    },
    "gr-tichu-card-5": {
        "type": Type.STRING,
        "nullable": False
    },
    "gr-tichu-card-6": {
        "type": Type.STRING,
        "nullable": False
    },
    "gr-tichu-card-7": {
        "type": Type.STRING,
        "nullable": False
    },
    "gr-tichu-card-8": {
        "type": Type.STRING,
        "nullable": False
    },
    "extra-card-1": {
        "type": Type.STRING,
        "nullable": False
    },
    "extra-card-2": {
        "type": Type.STRING,
        "nullable": False
    },
    "extra-card-3": {
        "type": Type.STRING,
        "nullable": False
    },
    "extra-card-4": {
        "type": Type.STRING,
        "nullable": False
    },
    "extra-card-5": {
        "type": Type.STRING,
        "nullable": False
    },
    "extra-card-6": {
        "type": Type.STRING,
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
    "start-card-1": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-2": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-3": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-4": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-5": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-6": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-7": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-8": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-9": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-10": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-11": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-12": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-13": {
        "type": Type.STRING,
        "nullable": False
    },
    "start-card-14": {
        "type": Type.STRING,
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