import traceback
from pyspark.sql import SparkSession
import time

"""Create the SparkSession and connect to the MongoDB Cloud Database through MongoDB uri"""


def Initialize():
    try:
        # Create a SparkSession
        spark = SparkSession.builder \
            .appName("PercentageBoosterPrevious Query") \
            .config("spark.mongodb.read.connection.uri",
                    "mongodb+srv://mrpingitore:lGvBxp9uVDtkQXNJ@cluster0.givyv2u.mongodb.net/BDMProject.PercentageBoosterPrevious") \
            .config("spark.mongodb.write.connection.uri",
                    "mongodb+srv://mrpingitore:lGvBxp9uVDtkQXNJ@cluster0.givyv2u.mongodb.net/BDMProject.PercentageBoosterPrevious") \
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


"""Create the query in SQL Syntax: Find the regions with the highest percentage of individuals who received 
the booster dose who had a previous infection, considering only regions that have been provided with more than one 
supplier and ordered based on the percentage:"""
def DefineQuery(dataDf):
    # Register the DataFrame as a temporary view
    dataDf.createOrReplaceTempView("dataset")

    # Define the query to be executed
    query = """
        SELECT region_name, (SUM(CASE WHEN additional_booster_dose > 0 AND previous_infection > 0 
        THEN 1 ELSE 0 END) / SUM(CASE WHEN previous_infection > 0 THEN 1 ELSE 0 END)) * 100 AS percentage, 
        COLLECT_LIST(DISTINCT supplier)AS suppliers
        FROM dataset
        WHERE region IN (
            SELECT region
            FROM (
                SELECT region, COUNT(DISTINCT supplier) AS supplier_count
                FROM dataset
                GROUP BY region
                HAVING supplier_count > 1
            ) AS regions_with_multiple_suppliers
        )
        GROUP BY region_name
        ORDER BY percentage DESC
    """
    return query


"""Execute the query and calculate the execution time of the query through two sampling of time, one just before the execution and one just after it finishes"""
def ExecuteQuery(query, spark):
    try:
        # start the timer
        startTime = time.time()

        # execute the query
        result = spark.sql(query)
        endTime = time.time() - startTime

        # show the results
        result.show(100000)
        print(f"Execution time: " + str(endTime))

        return result
    except Exception as e:
        print(traceback.format_exc())
        print(e)
        print("Execution error -> Cannot execute query")


"""Save the results to MongoDB by overwriting the content already stored"""
def SaveToMongoDB(result):
    try:
        # Write the result DataFrame to MongoDB
        result.write.format("mongodb").mode("overwrite").option("database", "BDMProject").option("collection", "PercentageBoosterPrevious").save()
        print("Results saved.")
    except Exception as e:
        print(traceback.format_exc())
        print(e)
        print("Saving error -> Cannot save result on MongoDB")


def main():
    spark = Initialize()
    dataDf = LoadData(spark)
    query = DefineQuery(dataDf)
    result = ExecuteQuery(query, spark)
    SaveToMongoDB(result)
    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    print("Starting PercentageBoosterPrevious Query")
    main()
    print("Elaboration Query concluded")
