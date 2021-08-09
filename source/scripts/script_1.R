SPARK_HOME = "D:/Spark"
Sys.setenv(SPARK_HOME=SPARK_HOME)
library(SparkR,
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "local[3]",
               sparkConfig = list(spark.driver.memory = "2g"))

data = read.df(
  path = "../data/used_cars_data.csv", 
  source = "csv",
  delimiter = ";",
  header = TRUE
)

createOrReplaceTempView(data, "enem")
collect(
  sql(
    "
    SELECT 
      TP_SEXO,
      COUNT(*) AS n
    FROM 
      enem
    GROUP BY
      TP_SEXO
    "
  )
)

superior = "../data/used_cars_data.csv"

data_spark = read.df(
  path = superior, 
  source = "csv",
  delimiter = "|",
  header = TRUE
)
schema(data_spark)

createOrReplaceTempView(data_spark, "superior")
head(
  sql(
    "
    SELECT 
      COUNT(*) AS n
    FROM 
      superior
    "
  )
)
# sparkR.session.stop()
