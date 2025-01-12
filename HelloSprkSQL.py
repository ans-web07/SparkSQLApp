import os
import sys

from pyspark import *
from pyspark.sql import *
from lib.logger import Log4J

if __name__ == "__main__":

    # Creating a new spark conf object
    conf1 = SparkConf()
    conf1.setMaster("local[3]") \
        .setAppName("Hello SparkSQL App")

    spark = SparkSession.builder \
        .config(conf=conf1) \
        .getOrCreate()
    logger = Log4J(spark)

    input_file = sys.argv[1]
    if not os.path.isfile(input_file):
        logger.error(f"Input file '{input_file}' does not exist or is not accessible.")
        sys.exit(-1)

    survey_df= spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sys.argv[1])
    logger.info("Survey_df result after reading .csv file")
    survey_df.show()
   #Creating a table/view via Dataframe
    survey_df.createOrReplaceTempView("survey_tbl")
    logger.info("Survey_tbl created via dataframe")

    # Executing SQL expression only on a table/view
    logger.info(" Executing SQL expression only on survey_tbl")
    count_df = spark.sql("select Country,count(1) as count from survey_tbl where Age<40 group by country")
    count_df.show()
