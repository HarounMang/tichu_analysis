from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType

from shared import process_text_file
from columns import COLUMNS, Type

spark_sql_type = {
    Type.STRING: StringType(),
    Type.INTEGER: IntegerType(),
    Type.FLOAT: FloatType(),
    Type.STRING_ARRAY: ArrayType(StringType(), containsNull=False)
}

def map_to_rows(rdd_entry: tuple[str, str]):
    game_id = int(rdd_entry[0].split("/")[-1].split(".")[0])
    return process_text_file(rdd_entry[1], game_id)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .getOrCreate()
    sc = spark.sparkContext

    rdd = sc.wholeTextFiles('/user/s2860406/dev_tichu')
    processed_rdd = rdd.flatMap(map_to_rows)

    schema = StructType([
        StructField(key, spark_sql_type[value["type"]], value["nullable"]) for key, value in COLUMNS.items() 
    ])

    df = spark.createDataFrame(processed_rdd, schema=schema)
    df.show()
    df.write.csv("./tichu_data", mode="overwrite")