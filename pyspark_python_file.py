
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("first_app").getOrCreate()


df=spark.read.csv(r"C:\\Users\\shahm\\OneDrive\\Documents\\Python Scripts\\pyspark-master\\data_git\\flight-data\csv\\2010-summary.csv",header=True,inferSchema=True,sep=",")
df.show()

df.createOrReplaceTempView("travel")

spark.sql("select * from travel")