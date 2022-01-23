import sys
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import Window

#Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)
                  
def main():
    # start Spark session
    spark = SparkSession.builder.appName("SparkDemo").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
                  
    logger.info("Starting spark application")
    logger.info("1. Reading Files") # mozliwe ze trzeba poprawic sciezki
    # z csv-ek
    # df_fic = spark.read.option("header","true").option("inferschema","true").csv("file:///home/vagrant/ao3/ao3_band_fic_metadata_clean.csv", sep=';').withColumn("date_updated", expr("""to_date(date_updated)"""))
    # df_tag = spark.read.option("header","true").option("inferschema","true").csv("file:///home/vagrant/ao3/ao3_band_tag_metadata_clean.csv", sep=';')
    # band = spark.read.option("header","true").option("inferschema","true").csv("file:///home/vagrant/spotify/bands.csv", sep=',') #dobry sep dobrac

    # z hdfs (parquet)
    df_fic = spark.read.option("header","true").option("inferschema","true").parquet("hdfs://127.0.0.1/user/vagrant/ao3/ao3_band_fic_metadata_clean.parquet").withColumn("date_updated", expr("""to_date(date_updated)"""))
    df_tag = spark.read.option("header","true").option("inferschema","true").parquet("hdfs://127.0.0.1/user/vagrant/ao3/ao3_band_tag_metadata_clean.parquet")
    band = spark.read.option("header","true").option("inferschema","true").parquet("hdfs://127.0.0.1/user/vagrant/spotify/bands.parquet")

    logger.info("2. Schema")
    df_fic.printSchema()
    df_tag.printSchema()
    band.printSchema()

    logger.info("3. Count")
    print('fic count', df_fic.count())
    print('tag count', df_tag.count())
    print('spotify band count', band.count())

    logger.info("4. Tag names")
    df_tag.select("meta_name").distinct().show()

    logger.info("5. Top 10 Fandoms")
    join1 = df_fic.join(df_tag.filter(f.col('meta_name')=='fandoms'), on='id', how='left')
    join1 = join1.distinct().groupBy('meta_val').agg({'id':'count', 'kudos':'sum', 'comments':'sum'}).sort("count(id)", ascending=False)
    join2 = join1.withColumn("band", f.col("meta_val")).join(band.withColumn("band", f.col("BandName")).select('band', 'Followers'), on="band", how='inner').select('band', 'Followers', 'sum(kudos)', 'sum(comments)', 'count(id)').sort("Followers", ascending=False)
    join2.show()
    join2.createOrReplaceTempView('Top10Fandoms')

    logger.info("6. Top Ships")
    w = Window.partitionBy('relationship')
    # sprawdzanie w jakim fandomie najczesciej znajduje sie ship
    join3 = df_tag.filter(f.col('meta_name')=='fandoms').withColumn("fandom", f.col('meta_val')).select('id', 'fandom').join(df_tag.filter(f.col('meta_name')=='relationships').withColumn("relationship", f.col('meta_val')).select('id', 'relationship'), on='id', how='inner').distinct()
    join3 = join3.groupby("fandom", "relationship").count()
    join3 = join3.withColumn('maxF', f.max('count').over(w)).where(f.col('count')==f.col('maxF')).drop('maxF').sort("count",     ascending=False).dropDuplicates(['relationship']).sort("count", ascending=False).drop('count')
    # liczenie statystyk dla danego shipu i doklejanie fandomu
    join4 = df_fic.join(df_tag.filter(f.col('meta_name')=='relationships'), on='id', how='inner')
    join4 = join4.distinct().withColumn("relationship", f.col('meta_val')).groupBy('relationship').agg({'id':'count', 'kudos':'sum', 'comments':'sum'})
    join4 = join4.join(join3, on='relationship', how='inner').sort("count(id)", ascending=False)
    join4.show(truncate=False)
    join4.createOrReplaceTempView('TopShips')

  
    # end Spark session
    logger.info("Ending spark application")
    spark.stop()
    
    return None

# Starting point for PySpark
if __name__ == '__main__':
    main()
    sys.exit()