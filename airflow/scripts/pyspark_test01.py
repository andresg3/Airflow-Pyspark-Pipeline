import pyspark

spark = pyspark.sql.SparkSession.builder \
    .appName('hogwarts') \
    .getOrCreate()

characters = [
    ("Albus Dumbledore", 150),
    ("Minerva McGonagall", 70),
    ("Rubeus Hagrid", 63),
    ("Oliver Wood", 18),
    ("Harry Potter", 12),
    ("Ron Weasley", 12),
    ("Hermione", 13),
    ("Draco Malfoy", None)
]

c_df = spark.createDataFrame(characters, ["name", "age"])

c_df.show()
