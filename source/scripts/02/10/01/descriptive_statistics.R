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

# Verificando tipo de objeto
avg(data$speed)
class(avg(data$speed))

# obtendo a média das velocidades
collect(
  select(data, avg(data$speed))
)

# Obtendo: média, variância e mediana das velocidades
collect(
  select(data, 
         avg(data$speed),
         var(data$speed), 
         percentile_approx(data$speed, percentage=.5))
)

# Obtendo a média da velocidade usando a função agg
collect(
  agg(data, speed = "avg") # Coluna = "avg, variance, etc"
)

# Nome da nova coluna como argumento seguida da operação desejada
collect(
  agg(data, media_da_velocidade = avg(data$speed))
)

################################################################################
########################### AGREGAÇÕES POR GRUPOS ##############################
################################################################################

collect(
  agg(groupBy(data, "type"), # Agrupamento pela coluna "type"
      media_tamanho = avg(data$size), 
      media_velocidade = avg(data$speed))
)