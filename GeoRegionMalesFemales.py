import traceback
from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import col, when, avg


"""Create the SparkSession and connect to the MongoDB Cloud Database through MongoDB uri"""
def Initialize():
    try:
        # Create the  SparkSession
        spark = SparkSession.builder \
            .appName("GeoRegionMalesFemales Query") \
            .config("spark.mongodb.read.connection.uri",
                    "mongodb+srv://mrpingitore:lGvBxp9uVDtkQXNJ@cluster0.givyv2u.mongodb.net/BDMProject.GeoRegionMalesFemales") \
            .config("spark.mongodb.write.connection.uri",
                    "mongodb+srv://mrpingitore:lGvBxp9uVDtkQXNJ@cluster0.givyv2u.mongodb.net/BDMProject.GeoRegionMalesFemales") \
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


"""Create the query in SQL Syntax: Average number of young (age between 12 and 29) male and female who had the additional booster dose in each geographic region of Italy
Execute the query and calculate the execution time of the query through two sampling of time"""
def ExecuteQuery(dataDf):
    # start the timer
    startTime = time.time()

    # group the region in geographic zone
    geoZone = dataDf.withColumn("geo_region",
                                when(col("region_name").isin("Valle d'Aosta", "Liguria", "Lombardia", "Piemonte",
                                                             "Trentino-Alto Adige", "Veneto", "Friuli-Venezia Giulia",
                                                             "Emilia-Romagna", "Toscana"), "North")
                                .when(col("region_name").isin("Umbria", "Marche", "Lazio", "Abruzzo", "Molise",
                                                              "Campania", "Puglia", "Basilicata", "Calabria"), "South")
                                .when(col("region_name").isin("Sicilia", "Sardegna"), "Islands")
                                .otherwise("Autonomous Provinces"))

    # calculate for each geographic zone, the average number of young males who received the additional booster dose
    avgMales = geoZone.filter((col("age_range").isin("12-19", "20-29")) & (col("additional_booster_dose") > 1)) \
        .groupBy("geo_region") \
        .agg(avg(when(col("age_range").isin("12-19", "20-29"), col("males"))).alias("avg_males"))

    # calculate for each geographic zone, the average number of young females who received the additional booster dose
    avgFemales = geoZone.filter((col("age_range").isin("12-19", "20-29")) & (col("additional_booster_dose") > 1)) \
        .groupBy("geo_region") \
        .agg(avg(when(col("age_range").isin("12-19", "20-29"), col("females"))).alias("avg_females"))

    # calculate the time elapsed
    endTime = time.time() - startTime

    # show the results
    avgMales.show()
    avgFemales.show()
    print(f"Execution time: " + str(endTime))

    return avgMales, avgFemales


"""Save the results to MongoDB by overwriting the content already stored"""
def SaveToMongoDB(avgMales, avgFemales):
    try:
        # Write the results DataFrame to MongoDB
        avgMales.write.format("mongodb").mode("overwrite").option("database", "BDMProject").option("collection", "GeoRegionMalesFemales").save()
        avgFemales.write.format("mongodb").mode("append").option("database", "BDMProject").option("collection", "GeoRegionMalesFemales").save()
        print("Results saved.")
    except Exception as e:
            print(traceback.format_exc())
            print(e)
            print("Saving error -> Cannot save result on MongoDB")


def main():
    spark = Initialize()
    dataDf = LoadData(spark)
    avgMales, avgFemales = ExecuteQuery(dataDf)
    SaveToMongoDB(avgMales, avgFemales)
    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    print("Starting GeoRegionMalesFemales Query")
    main()
    print("Elaboration Query concluded")
