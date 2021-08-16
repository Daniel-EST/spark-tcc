SPARK_HOME = "D:/Spark"

Sys.setenv(SPARK_HOME=SPARK_HOME)

library(SparkR, 
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"))) 

sparkR.session(master = "local[3]", 
               sparkConfig = list(spark.driver.memory = "2g"))

sparkR.version()

# Leitura do arquivo CSV dos dados
data = read.df(
  path = "../../../data/particles.csv", 
  source = "csv",
  delimiter = ",",
  inferSchema = "true",
  header = TRUE
)

head(data)
