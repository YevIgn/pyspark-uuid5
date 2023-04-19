import random
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

LENGTH = 100000
WORDS = ["lorem", "ipsum", "dolor", "sit", "amet", "foo", "bar", "baz", "42"]
OUTPUT_PATH = str((Path.cwd() / "string_data").as_posix())
DATA = [dict(c1=f"{i+1}", c2=random.choice(WORDS), c3=random.choice(WORDS)) for i in range(LENGTH)]

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(
    pd.DataFrame.from_records(
        data=DATA,
    )
).coalesce(1)

df.write.json(path=OUTPUT_PATH, mode="overwrite")
