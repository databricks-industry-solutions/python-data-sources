from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

def create_spark_session(app_name="PySpark CSV Zips Datasource Tester"):
    """
    Creates and returns a Spark Session
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )

def create_zipfile(spark):
    import os
    import zipfile

    csv_dir = "resources/x"
    zip_path = "resources/x.zip"

    spark.dataSource.register(ZipDataSource)
    spark.range(100).write.format("csv").mode('overwrite').save(csv_dir)
    os.makedirs(os.path.dirname(zip_path), exist_ok=True)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for filename in os.listdir(csv_dir):
            if filename.endswith(".csv"):
                file_path = os.path.join(csv_dir, filename)
                zipf.write(file_path, arcname=filename)
    

if __name__ == "__main__":
    print("Hello")
    from dbx.zip_csv_datasource import ZipDataSource
    
    spark = create_spark_session()
    create_zipfile(spark)

    df = spark.read.format("zipcsv").load("./resources/x.zip")
    df.limit(5).show(truncate=False)
    result = df.count()
    print(result)
    
    
    df = spark.read.format("zipcsv").load("./resources")
    df.limit(5).show(truncate=False)
    result = df.count()
    print(result)