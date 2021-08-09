SPARK_HOME = "D:/Spark"
Sys.setenv(SPARK_HOME=SPARK_HOME)
library(SparkR,
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "local[*]",
               sparkConfig = list(spark.driver.memory = "4g"))

data = read.df(
  path = "../data/used_cars_data.csv", 
  source = "csv",
  delimiter = ",",
  header = TRUE
)

head(data)
schema(data)

createOrReplaceTempView(data, "used_cars")
data = withColumn(data, "price", cast(data$price, "float"))

schema(data)
collect(
  sql(
    "
    SELECT 
      COUNT(*) AS n
    FROM 
      used_cars
    "
  )
)

hist = histogram(data, "price", 10000)

collect(
  sql(
    "
    SELECT 
      MAX(price) as max_price,
      MIN(price) as min_price
    FROM 
      used_cars
    "
  )
)

collect(agg(data, max(data$price), min(data$price)))

require(ggplot2)
plot = ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity") +
  xlab("Salário") + ylab("Frequência")
plot + xlim(-1000, 100)

splitted = randomSplit(data, c(0.9, 0.1))
train = splitted[[1]]
test = splitted[[2]]

createOrReplaceTempView(train, "train")
createOrReplaceTempView(test, "test")

count(train)
count(test)

collect(
  select(
    count(
      group_by(data, "interior_color")
      ),
    col = "interior_color"
    )
  )

collect(
  sql(
    "
    SELECT 
      interior_color,
      COUNT(*) AS n
    FROM 
      used_cars
    GROUP BY interior_color
    "
  )
)

# lit()
# 
# rdd()
# 
# sampleBy()



collect(
  sql(
    "
    SELECT 
      COUNT(negative_price) AS n,
    CASE 
      WHEN price <= 0 THEN 1
      ELSE 0
    END AS negative_price
    FROM 
      used_cars
    GROUP BY negative_price
    "
  )
)




