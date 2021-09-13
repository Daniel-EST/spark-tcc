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
df_list = randomSplit(data, c(7,3), seed = 210828)
df_list

train = df_list[[1]]
test = df_list[[2]]

# Treinammento do modelo
model = spark.lm(data = train, size ~ speed)
summary(model)

# Utilizando o modelo para prever os resultados dentro da amostra treino
predictions = predict(model, train)
head(predictions)

# Calculando a média da variável resposta
y_avg = collect(agg(predictions, avg(predictions$label)))[[1]]

# Calculando SQ_res e SQ_tot
df =
  agg(predictions,
      sq_res = sum((predictions$label - predictions$prediction)^2),
      sq_tot = sum((predictions$label - y_avg)^2))

SSR = collect(select(df, df$sq_res))[[1]]
SST = collect(select(df, df$sq_tot))[[1]]

# Calculando R²
Rsq = 1 - (SSR/SST)

# Obtendo o número de parâmetros do modelo
p = summary(model)$numFeatures + 1

# Número de observações
n = nrow(predictions)

# Calculando o R² Ajustado
aRsq = 1 - (((1 - Rsq)*(n - 1))/(n - p))

sprintf("R²: %.5f", Rsq)
sprintf("R²Ajustado: %.5f", aRsq)

# Salvando o modelo
write.ml(model, "./model/lm_particles")

# Carregando o modelo
model_loaded = read.ml("./model/lm_particles")