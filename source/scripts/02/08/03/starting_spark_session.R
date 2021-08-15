# Local de instalação do Spark.
SPARK_HOME = "D:/Spark"

# Cria uma variável de ambiente com informação do local de instalação do Spark.
Sys.setenv(SPARK_HOME=SPARK_HOME)

# Carrega o SparkR.
library(SparkR, 
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"))) 

# Inicia um cluster Local utilizando 3 cores e 2Gb de memória.
sparkR.session(master = "local[3]", 
               sparkConfig = list(spark.driver.memory = "2g"))

# Checa a versão do Spark que esta iniciada. 
sparkR.version() 
