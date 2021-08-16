SPARK_HOME = "D:/Spark"

Sys.setenv(SPARK_HOME=SPARK_HOME)

library(SparkR, 
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"))) 

sparkR.session(master = "local[3]", 
               sparkConfig = list(spark.driver.memory = "2g"))

sparkR.version()

funcionarios = data.frame(
  id = c(3, 4, 1, 5, 2), 
  nome = c("Luiz", "Lyncoln", "Gabriel", "Rodolfo", "Leticia"),
  salario = c(1175.70, 2023.6126, 3116.4971, 1490.11, 2252.9465), 
  data_de_contratacao = as.Date(c("2013-10-11", "2012-07-20", "2018-07-05", "2017-08-13", "2018-07-05"))
)

funcionarios_spark = createDataFrame(funcionarios)

# Criando condção para coluna identificadora de funcionarios contratados até o ano de 2015
condicao = ifelse(funcionarios_spark$data_de_contratacao > "2015-01-01", 0, 1)
condicao

# Criar uma coluna nomeada de "veterano", indicadora de contratação anterior ao ano de 2015.
funcionarios_spark = withColumn(funcionarios_spark, "veterano", condicao)
collect(funcionarios_spark)
