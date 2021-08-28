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

predictions = predict(model, train)
head(predictions)

y_avg = collect(agg(predictions, y_avg = mean(predictions$label)))$y_avg

df = transform(predictions, 
               y_hat = predictions$prediction, 
               sq_res = (predictions$label - predictions$prediction)^2, 
               sq_tot = (predictions$label - y_avg)^2, 
               res = predictions$label - predictions$prediction)

df$prediction = NULL

head(select(df, "label", "y_hat", "sq_res", "sq_tot"))
SSR = collect(agg(df, SSR = sum(df$sq_res)))
SST = collect(agg(df, SST = sum(df$sq_tot)))

Rsq = 1-(SSR[[1]]/SST[[1]])
p = 10
N = nrow(df)
aRsq = Rsq - (1 - Rsq)*((p - 1)/(N - p))

# Salvando o modelo
write.ml(model, "./model/lm_particles")

model_loaded = read.ml("./model/lm_particles")

# Comparar o modelo carregado
summary(model)
summary(model_loaded)
