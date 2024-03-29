from pyspark.sql import *
from pyspark.sql.functions import *

if __name__ == '__main__':

    spark = SparkSession.builder.appName("Assignment-7").getOrCreate()

    schema = "order_id int,customer_id int,restaurant_id int,order_time string,delivery_time string," \
             "customer_location string,restaurant_location string,order_value string,rating int"

    df = spark.read \
            .format("csv") \
            .schema(schema) \
            .option("header", "true") \
            .option("mode", "FAILFAST") \
            .load("s3://gds-assignment-7-tavish/landing_zone/")

    df = df.withColumn("order_time", to_timestamp("order_time")) \
            .withColumn("delivery_time", to_timestamp("delivery_time")) \
            .withColumn("order_value", abs("order_value"))

    df.printSchema()
    df.show(truncate=False)

    transformedDf = df.withColumn("category", when(col("order_value") > 10, "Medium") \
                                  .when(col("order_value") > 10, "High").otherwise("Low")) \
                        .withColumn('duration', (col('delivery_time').cast('long') - col('order_time').cast('long'))/60)

    transformedDf.show(truncate=False)

    transformedDf.write \
        .format("jdbc") \
        .option("url", "jdbc:redshift://redshift-cluster-1.c3py6xstirpi.ap-south-1.redshift.amazonaws.com:5439/dev") \
        .option("dbtable", "assignment_7") \
        .option("user", "admin") \
        .option("password", "KunAguero10") \
        .mode("overwrite") \
        .save()

    print("JOB DONE")