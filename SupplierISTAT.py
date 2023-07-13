import traceback
from pyspark.sql import SparkSession
import time

"""Create the SparkSession and connect to the MongoDB Cloud Database through MongoDB uri"""
def Initialize():
    try:
        # Create a SparkSession
        spark = SparkSession.builder \
            .appName("Dataset Query") \
            .config("spark.mongodb.read.connection.uri",
                    "mongodb+srv://mrpingitore:lGvBxp9uVDtkQXNJ@cluster0.givyv2u.mongodb.net/BDMProject.SupplierISTAT") \
            .config("spark.mongodb.write.connection.uri",
                    "mongodb+srv://mrpingitore:lGvBxp9uVDtkQXNJ@cluster0.givyv2u.mongodb.net/BDMProject.SupplierISTAT") \
            .getOrCreate()

        return spark
    except Exception as e:
        print(traceback.format_exc())
        print(e)
        print("Spark error -> Cannot initialize SparkSession")


"""Load the dataset into the dataframe"""
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


"""Create the query in SQL Syntax : for each supplier, identify in how main ISTAT regions  has been utilized, on how many adult (age range between 30-89) males and females, and what is the ISTAT region in which that particular supplier has been more utilized"""
def DefineQuery(dataDf):
    # Register the DataFrame as a temporary view
    dataDf.createOrReplaceTempView("dataset")

    # Define the query to be executed
    query = """
    WITH supplier_stats AS (
        SELECT
            supplier,
            ISTAT_regional_code,
            COUNT(*) AS count
        FROM
            dataset
        GROUP BY
            supplier, ISTAT_regional_code
    ), supplier_max_ISTAT AS (
        SELECT
            supplier,
            ISTAT_regional_code,
            ROW_NUMBER() OVER (PARTITION BY supplier ORDER BY count DESC) AS rn
        FROM
            supplier_stats
    )
    SELECT
        s.supplier,
        s.region_count,
        s.total_males,
        s.total_females,
        m.ISTAT_regional_code AS most_frequent_ISTAT
    FROM (
        SELECT
            supplier,
            COUNT(DISTINCT ISTAT_regional_code) AS region_count,
            SUM(CASE WHEN age_range IN ('30-39', '40-49', '50-59', '60-69', '80-89') AND males > 0 THEN males ELSE 0 END) AS total_males,
            SUM(CASE WHEN age_range IN ('30-39', '40-49', '50-59', '60-69', '80-89') AND females > 0 THEN females ELSE 0 END) AS total_females
        FROM
            dataset
        GROUP BY
            supplier
    ) s
    JOIN supplier_max_ISTAT m ON s.supplier = m.supplier AND m.rn = 1
    ORDER BY
        s.total_males + s.total_females DESC
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
        result.write.format("mongodb").mode("overwrite").option("database", "BDMProject").option("collection", "SupplierISTAT").save()
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
    print("Starting Elaboration Query")
    main()
    print("Elaboration Query concluded")
