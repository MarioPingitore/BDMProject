import traceback
from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import col


"""Create the SparkSession and connect to the MongoDB Cloud Database through MongoDB uri"""
def Initialize():
    try:
        # Create a SparkSession
        spark = SparkSession.builder \
            .appName("MaxFirstSecond Query") \
            .config("spark.mongodb.read.connection.uri",
                    "mongodb+srv://mrpingitore:lGvBxp9uVDtkQXNJ@cluster0.givyv2u.mongodb.net/BDMProject.MaxFirstSecond") \
            .config("spark.mongodb.write.connection.uri",
                    "mongodb+srv://mrpingitore:lGvBxp9uVDtkQXNJ@cluster0.givyv2u.mongodb.net/BDMProject.MaxFirstSecond") \
            .getOrCreate()

        return spark
    except Exception as e:
        print(traceback.format_exc())
        print(e)
        print("Spark error -> Cannot initialize SparkSession")


"""Load the dataset into the Spark dataframe"""
def LoadData(spark):
    try:
        # Load the dataset
        dataset_path = "data/italian_vaccination.csv"
        df = spark.read.csv(dataset_path, header=True, inferSchema=True)

        return df
    except Exception as e:
        print(traceback.format_exc())
        print(e)
        print("Loading error -> Cannot load dataset")


"""Create the query in SQL Syntax: the 100 administration date based on maximization of cumulative number of first_dose + second_dose for for very old people (age 90+) and children (0-12), along with the region in which the max record for the vaccine administration has been registered.
Execute the query and calculate the execution time of the query through two sampling of time"""
def ExecuteQuery(dataDf):
    # start the timer
    startTime = time.time()

    # retrieve the top 100 of the administration_date of the max administration of first_dose for children and very old people and the region in which the record has been registered
    maxFirstDose = dataDf.filter((col("age_range") == "0-12") | (col("age_range") == "90+")) \
        .orderBy(col("first_dose").desc()) \
        .withColumnRenamed("region", "region_first_dose") \
        .select("administration_date", "first_dose", "region_first_dose") \
        .limit(100)

    # retrieve the top 100 of the administration_date of the max administration of second_dose for children and very old people and the region in which the record has been registered
    maxSecondDose = dataDf.filter((col("age_range") == "0-12") | (col("age_range") == "90+")) \
        .orderBy(col("second_dose").desc()) \
        .withColumnRenamed("region", "region_second_dose") \
        .select("administration_date", "second_dose", "region_second_dose") \
        .limit(100)

    # Combine the two dataframe obtained by the subqueries through a join and order based on the max cumulative value of first_dose + second_dose
    maxFirstSecond = maxFirstDose.join(maxSecondDose, on="administration_date", how="inner").distinct(). \
        orderBy((col("first_dose") + col("second_dose")).desc())

    # calculate the time elapsed
    endTime = time.time() - startTime

    # show the results
    maxFirstDose.show(100000)
    maxSecondDose.show(100000)
    maxFirstSecond.show(100000)
    print(f"Execution time: " + str(endTime))

    return maxFirstDose, maxSecondDose, maxFirstSecond


"""Save the results to MongoDB by overwriting the content already stored: each query save its result in a different collection of the database"""
def SaveToMongoDB(maxFirstDose, maxSecondDose, maxFirstSecond):
    try:
        # Write the results DataFrame to MongoDB
        maxFirstDose.write.format("mongodb").mode("overwrite").option("database", "BDMProject").option("collection", "MaxFirstSecond_firstDose").save()
        maxSecondDose.write.format("mongodb").mode("overwrite").option("database", "BDMProject").option("collection", "MaxFirstSecond_secondDose").save()
        maxFirstSecond.write.format("mongodb").mode("overwrite").option("database", "BDMProject").option("collection", "MaxFirstSecond").save()
        print("Results saved.")
    except Exception as e:
        print(traceback.format_exc())
        print(e)
        print("Saving error -> Cannot save results on MongoDB")


def main():
    spark = Initialize()
    dataDf = LoadData(spark)
    maxFirstDose, maxSecondDose, maxFirstSecond = ExecuteQuery(dataDf)
    SaveToMongoDB(maxFirstDose, maxSecondDose, maxFirstSecond)
    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    print("Starting MaxFirstSecond Query")
    main()
    print("Elaboration Query concluded")
