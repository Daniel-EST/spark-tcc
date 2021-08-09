SPARK_HOME = "D:/Spark"
Sys.setenv(SPARK_HOME=SPARK_HOME)
library(SparkR,
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "local[3]",
               sparkConfig = list(spark.driver.memory = "2g"))

sparkR.version()

funcionarios = data.frame(
  id = c(3, 4, 1, 5, 2), 
  nome = c("Luiz", "Lyncoln", "Gabriel", "Rodolfo", "Letícia"),
  salario = c(1175.70, 2023.6126, 3116.4971, 1490.11, 2252.9465), 
  data_de_contratacao = as.Date(c("2013-10-11", "2012-07-20", "2018-07-05", "2017-08-13", "2018-07-05"))
)

funcionarios_spark = createDataFrame(funcionarios, numPartitions = 10)

funcionarios
funcionarios_spark

class(funcionarios)
class(funcionarios_spark)

head(funcionarios_spark)

collect(
  select(funcionarios_spark, "nome")
)

collect(
  filter(funcionarios_spark, "salario <= 2000")
)

collect(
  arrange(funcionarios_spark, "id")
)

createOrReplaceTempView(funcionarios_spark, "funcionarios")
collect(
  sql(
    "
  SELECT *,
  CASE
    WHEN salario > 2000 THEN 1
    ELSE 0
  END AS salario_cat
  FROM funcionarios
    "
  )
)

condicao = ifelse(funcionarios_spark$salario > 2000, 1, 0)
funcionarios_spark = withColumn(funcionarios_spark, "salario_cat", condicao)
collect(funcionarios_spark)

condicao = ifelse(funcionarios_spark$data_de_contratacao > "2015-01-01", 0, 1)
condicao

funcionarios_spark = withColumn(funcionarios_spark, "veterano", condicao)
collect(funcionarios_spark)

collect(
  summarize(
    group_by(funcionarios_spark, "veterano"),
    mean_salary = mean(funcionarios_spark$salario)
  )
)

histogram(funcionarios_spark, "salario")

data_splits = randomSplit(funcionarios_spark, c(0.8, 0.2))
data_splits = randomSplit(funcionarios_spark, c(0.5, 0.5))

head(data_splits[[1]])
head(data_splits[[2]])


hist = histogram(funcionarios_spark, "salario")

require(ggplot2)
plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity") +
  xlab("Salário") + ylab("Frequência")






































data(iris)
require(dplyr)
altered = iris

altered = altered %>% 
  select(Sepal.Length, Sepal.Width, Species) %>% 
  rename(speed = Sepal.Length, size = Sepal.Width, type = Species) %>% 
  mutate(speed = speed + runif(length(speed), min = 1.4, max = 2.2), 
         size = size  * runif(length(size), min=1.3, max=2.3)) %>% 
  mutate(type = case_when(
    type == "setosa" ~ "A",
    type == "versicolor" ~ "B",
    type == "virginica" ~ "C"
    )) %>% 
  mutate_if(~is.numeric(.x), ~round(.x, 2))

altered = rbind(altered , c(NA, 3, "C"))
altered = rbind(altered , c(NA, NA, "A"))
altered = rbind(altered , c(2, NA, "B"))

altered = altered %>% 
  arrange(type)


write.csv(altered, "example.csv")

funcionarios = data.frame(
  id = c(3, 4, 1, 5, 2), 
  nome = c("Luiz", "Lyncoln", "Gabriel", "Rodolfo", "Letícia", "A", "B", "C"),
  salario = c(1175.70, 2023.6126, 3116.4971, 1490.11, 2252.9465), 
  data_de_contratacao = as.Date(c("2013-10-11", "2012-07-20", "2018-07-05", "2017-08-13", "2018-07-05"))
)
