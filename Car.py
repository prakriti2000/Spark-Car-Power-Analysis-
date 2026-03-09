from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

if __name__ == "__main__":
    #creating a spark session called car analysis
    spark = SparkSession.builder.appName("CarAnalysis").getOrCreate()

    # data provided
    data = [("Ford Torino",140,3449,"US"),
    ("Chevrolet Monte Carlo",150,3761,"US"), ("BMW 2002",113,2234,"Europe")]

    #metadata - this tells spark the column names and their data type
    schema = StructType([
        StructField("carr", StringType(), True),
        StructField("horsepower", IntegerType(), True),
        StructField("weight", IntegerType(), True),
        StructField("origin", StringType(), True)
    ])

    df = spark.createDataFrame(data=data, schema=schema)

    df.printSchema()
    df.show()

    df1 = df.withColumnRenamed("carr", "car")

    df2 = df1.withColumn("AvgWeight", lit(200))

    df3 = df2.withColumn("kilowatt_power", col("horsepower") * 1000)

    df3.printSchema()
    df3.show(truncate=False) #truncate+false will ensure that long values are not cut off