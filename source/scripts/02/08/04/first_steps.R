SPARK_HOME = "D:/Spark"

Sys.setenv(SPARK_HOME=SPARK_HOME)

library(SparkR, 
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"))) 

sparkR.session(master = "local[3]", 
               sparkConfig = list(spark.driver.memory = "2g"))

sparkR.version() 

# Criar um pequeno conjunto de dados.
funcionarios = data.frame(
  id = c(3, 4, 1, 5, 2), 
  nome = c("Luiz", "Lyncoln", "Gabriel", "Rodolfo", "Leticia"),
  salario = c(1175.70, 2023.6126, 3116.4971, 1490.11, 2252.9465), 
  data_de_contratacao = as.Date(c("2013-10-11", "2012-07-20", "2018-07-05", "2017-08-13", "2018-07-05"))
)
# Criar um DataFrame do Spark, a partir de um objeto do tipo data.frame do R.
funcionarios_spark = createDataFrame(funcionarios)

# Mostrar no console a difereça entre os conjuntos de dado.
funcionarios
funcionarios_spark

# Verificando diferenças das classes
class(funcionarios)
class(funcionarios_spark)

# Obtendo as duas primeiras linhas de um SparkDataFrame
head(funcionarios_spark, n = 2)

# Obtendo todos os dados de um SparkDataFrame
collect(funcionarios_spark)
