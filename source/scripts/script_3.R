SPARK_HOME = "D:/Spark"
Sys.setenv(SPARK_HOME=SPARK_HOME)
library(SparkR,
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "local[*]",
               sparkConfig = list(spark.driver.memory = "4g"))

# data = read.df(
#   path = "../data/used_cars_data.csv", 
#   source = "csv",
#   delimiter = ",",
#   header = TRUE
# )

data = read.df(
  path = "example.csv", 
  source = "csv",
  delimiter = ",",
  header = TRUE
)

head(data)

collect(
  select(data, avg(data$speed))
)

avg(data$speed)
class(avg(data$speed))

collect(
  select(data, 
         avg(data$speed), 
         avg(data$size)
  )
)

collect(
  select(data, 
         avg(data$speed),
         var(data$speed), 
         percentile_approx(data$speed, percentage=.5)
         )
  )


collect(select(data, percentile_approx(data$speed, .5)))


collect(
  agg(
    groupBy(data, "type"), 
    size_mean = avg(data$size),
    size_var = var(data$size)
    )
  )

collect(
  agg(data, speed = "avg")
)
collect(
  agg(data, media_da_velocidadee = avg(data$speed))
)

collect(
  agg(groupBy(data, "type"), 
      media_tamanho = avg(data$size), 
      media_velocidade = avg(data$speed))
)

boxplot = collect(
  agg(groupBy(data, "type"), 
      med = percentile_approx(data$speed, percentage=.5), 
      fst = percentile_approx(data$speed, percentage=.25),
      trd = percentile_approx(data$speed, percentage=.75),
      max = max(data$speed),
      min = min(data$speed))
)

boxplot = collect(
  agg(groupBy(data, "type"), 
      med = percentile_approx(data$speed, percentage=.5), 
      fst = percentile_approx(data$speed, percentage=.25),
      trd = percentile_approx(data$speed, percentage=.75))
)


require(ggplot2)
hist = histogram(data, "size")
hist

boxplot = collect(
  agg(groupBy(data, "type"), 
      med = percentile_approx(data$speed, percentage=.5), 
      fst = percentile_approx(data$speed, percentage=.25),
      trd = percentile_approx(data$speed, percentage=.75))
)

ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity", col = "black") +
  xlab("Velocidade") + ylab("Frequência")

ggplot(boxplot, 
       aes(x = type, 
           ymin = fst - 1.5 * (trd - fst), 
           lower = fst, 
           middle = med, 
           upper = trd,  
           ymax = trd + 1.5 * (trd - fst))) +
  geom_boxplot(stat = "identity")

