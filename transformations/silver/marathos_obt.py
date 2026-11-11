from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.table(
    name="marathos.silver.marathos_obt",
    comment="Cleaned Marathos one big table"
)
def marathos_obt():
    df = spark.sql("FROM STREAM marathos.bronze.results_raw")

    df_clean = (
        df
        .withColumnRenamed("Year of event", "event_year")
        .withColumnRenamed("Event dates", "event_dates")
        .withColumnRenamed("Event name", "event_name")
        .withColumnRenamed("Event distance/length", "event_distance_length")
        .withColumnRenamed("Event number of finishers", "event_number_of_finishers")
        .withColumnRenamed("Athlete performance", "athlete_performance")
        .withColumnRenamed("Athlete club", "athlete_club")
        .withColumnRenamed("Athlete country", "athlete_country")
        .withColumnRenamed("Athlete year of birth", "athlete_year_of_birth")
        .withColumnRenamed("Athlete gender", "athlete_gender")
        .withColumnRenamed("Athlete age category", "athlete_age_category")
        .withColumnRenamed("Athlete average speed", "athlete_average_speed")
        .withColumnRenamed("Athlete ID", "athlete_id")
    )

    df_clean = (
        df_clean
        .filter(~col("athlete_performance").contains("d"))
        .filter(~col("event_distance_length").contains("Etappen"))
        .withColumn(
            "event_type",
            when(col("event_distance_length").contains("km"), "distance")
            .when(col("event_distance_length").contains("mi"), "distance")
            .when(col("event_distance_length").contains("h"), "time")
            .otherwise("unknown")
        )
        .filter(col("event_type") != "unknown")
        .filter(
            ~(
                (col("event_type") == "distance") &
                (~col("athlete_performance").contains("h"))
            )
        )
        .withColumn(
            "athlete_club",
            regexp_replace(
                when(col("athlete_club").isNull(), "Unknown")
                .otherwise(trim(col("athlete_club"))),
                r"\*",
                ""
            )
        )
        .withColumn(
            "athlete_average_speed",
            round(expr("try_cast(athlete_average_speed as double)"), 2)
        )
        .filter(
            col("athlete_average_speed").between(1, 25)
        )
        .withColumn(
            "event_date_clean",
            regexp_extract(col("event_dates"), r"(\d{2}\.\d{2}\.\d{4})$", 1)
        )
        .withColumn(
            "event_date",
            to_date(col("event_date_clean"), "dd.MM.yyyy")
        )
    )
    df_clean = (
        df_clean
        # An event is unique by name, date, year, distance and number of finishers.
        # The year is included because some dates could not be parsed and are null.
        # xxhash64 gives 64-bit ids, which avoids the collisions hash() (32-bit) caused.
        .withColumn(
            "event_id",
            abs(xxhash64(
                col("event_name"),
                col("event_date"),
                col("event_year"),
                col("event_distance_length"),
                col("event_number_of_finishers"),
            ))
        )
        .withColumn(
            "result_id",
            abs(xxhash64(
                col("event_id"), 
                col("athlete_id"), 
                col("athlete_performance"),
                col("athlete_year_of_birth"),
                col("athlete_club")
            ))
        )
        .withColumn(
            "athlete_year_of_birth",
            col("athlete_year_of_birth").cast("double")
        )
        .filter(
            col("athlete_year_of_birth").between(1720, 2004)
        )
        .withColumn(
            "athlete_age_category",
            when(col("athlete_age_category").isNull(), "Unknown")
            .otherwise(trim(col("athlete_age_category")))
        )
    )
    # Remove exact duplicate results from the source data
    df_clean = df_clean.dropDuplicates(["result_id"])
    df_clean = df_clean.drop("event_date_clean")

    return df_clean