SPARK_HOME = "D:/Spark"

Sys.setenv(SPARK_HOME=SPARK_HOME)

library(SparkR,
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "local[3]",
               sparkConfig = list(spark.driver.memory = "2g"))

data = read.df(
  path = "../../../../data/particles.csv", 
  source = "csv",
  delimiter = ",",
  inferSchema = "true",
  header = TRUE
)

# Verificando quantidade de dados faltantes
collect(
  agg(
    groupBy(data, 
            isNull(data$speed),
            isNull(data$size)),
    count(data$type)
    )
  )

# Calculando média para imputação dos dados
mean_speed = collect(select(data, avg(data$speed)))[[1]]
mean_size = collect(select(data, avg(data$size)))[[1]]

# Imputando dados faltantes respectivos as suas colunas
data = fillna(data, list("speed" = mean_speed, 
                         "size" = mean_size))

# Separação em treino e teste
df_list = randomSplit(data, c(7,3))
df_list

train = df_list[[1]]
test = df_list[[2]]

# Treinammento do modelo
model = spark.lm(data = train, size ~ speed)
summary(model)
