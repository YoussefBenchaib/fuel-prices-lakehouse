from pyspark.sql import SparkSession


def get_spark(app_name="fuel-prices-lakehouse"):
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        # Prices are reported in Italian local time.
        .config("spark.sql.session.timeZone", "Europe/Rome")
        # Overwrite only the partitions being written, so re-running a day replaces that day
        # and leaves the others alone.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # One day is ~100k rows: the default 200 shuffle partitions would mean 200 tiny tasks.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
