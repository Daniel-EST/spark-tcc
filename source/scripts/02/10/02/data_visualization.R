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

# Obtendo valores para realização para gráfico de barras
barplot_values = collect(count(groupBy(data, "type")))
barplot_values

require(ggplot2) # Importar pacote para criação de gráficos

ggplot(barplot_values, aes(x = type, y = count)) +
  geom_bar(stat = "identity", col = "black") +
  ggtitle("Quantidade de partículas por tipo") +
  xlab("Tipo") + ylab("Frequência")

# Obtendo valores para realização de um histograma
hist_values = histogram(data, "size", nbins = 10)
hist_values

ggplot(hist_values, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity", col = "black") +
  ggtitle("Histograma da velocidade das partículas") +
  xlab("Velocidade") + ylab("Frequência")

# Obtendo valores para realização para o boxplot
boxplot_values = collect(
  agg(groupBy(data, "type"), 
      med = percentile_approx(data$speed, percentage=.5), 
      q1 = percentile_approx(data$speed, percentage=.25),
      q3 = percentile_approx(data$speed, percentage=.75))
)
boxplot_values

ggplot(boxplot_values, 
       aes(x = type, 
           ymin = q1 - 1.5 * (q3 - q1),
           lower = q1, 
           middle = med, 
           upper = q3,  
           ymax = q3 + 1.5 * (q3 - q1))) +
  geom_boxplot(stat = "identity") +
  ggtitle("Boxplot da velocidade das partículas por tipo de partícula") +
  xlab("Tipo") + ylab("Velocidade")


# Código para criação de um histograma bivariado

# Definir a quantidade de bins
nbin = 13

# Calcular mínimo de x
x_min = collect(agg(data, min(data$size))) 
# Calcular máximo de x
x_max = collect(agg(data, max(data$size))) 
# Definir os intervalos para os bins de x
x_bin = seq(floor(x_min[[1]]), 
            ceiling(x_max[[1]]), 
            length = nbin) 

# Calcular mínimo de y
y_min = collect(agg(data, min(data$speed))) 
# Calcular máximo de y
y_max = collect(agg(data, max(data$speed))) 
# Definir os intervalos para os bins de y
y_bin = seq(floor(y_min[[1]]), 
            ceiling(y_max[[1]]), 
            length = nbin) 

# Calcular tamanho do intervalo dos bins de x e y
x_bin_width = x_bin[[2]] - x_bin[[1]] 
y_bin_width = y_bin[[2]] - y_bin[[1]] 

# Calcular a qual bin pertece cada valor observado
graph_data = withColumn(data, "x_bin", ceiling((data$size - x_min[[1]]) / x_bin_width)) 
graph_data = withColumn(graph_data, "y_bin", ceiling((data$speed - y_min[[1]]) / y_bin_width))
graph_data = mutate(graph_data, x_bin = ifelse(graph_data$x_bin == 0, 1, graph_data$x_bin))
graph_data = mutate(graph_data, y_bin = ifelse(graph_data$y_bin == 0, 1, graph_data$y_bin))

graph_data = collect(agg(groupBy(graph_data, "x_bin", "y_bin"), 
                         count = n(graph_data$x_bin)))

ggplot(graph_data, aes(x = x_bin, y = y_bin, fill = count)) + 
  geom_tile() + 
  scale_fill_distiller(palette = "Blues", direction = 1) +
  ggtitle("Velocidade x Tamanho") +
  xlab("Tamanho") + ylab("Velocidade")
