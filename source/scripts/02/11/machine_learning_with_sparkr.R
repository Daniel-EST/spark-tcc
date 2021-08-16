SPARK_HOME = "D:/Spark"
Sys.setenv(SPARK_HOME=SPARK_HOME)
library(SparkR,
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "local[*]",
               sparkConfig = list(spark.driver.memory = "4g"))

data = read.df(
  path = "../../../data/particles.csv", 
  source = "csv",
  delimiter = ",",
  inferSchema = "true",
  header = TRUE
)

# Preparando o banco de dados
collect(
  agg(
    groupBy(data, 
            isNull(data$speed),
            isNull(data$size)),
    count(data$type)
    )
  )


mean_speed = collect(select(data, avg(data$speed)))[[1]]
mean_size = collect(select(data, avg(data$size)))[[1]]

data = fillna(data, list("speed" = mean_speed, 
                         "size" = mean_size))

collect(
  agg(
    groupBy(data, 
            isNull(data$speed),
            isNull(data$size)),
    count(data$type)
  )
)

df_list = randomSplit(data, c(7,3))
df_list


train = df_list[[1]]
test = df_list[[2]]

train = data

total_data = collect(
  agg(data, count(data$type))
)[[1]]

count_data = collect(
  agg(
    groupBy(data, "type"),
    count(data$type),
    count(data$type)/total_data)
)

total_train= collect(
  agg(data, count(data$type))
)[[1]]

collect(
  agg(
    groupBy(train, "type"),
    count(train$type),
    count(train$type)/total_train)
)

total_test = collect(
  agg(data, count(data$type))
)[[1]]

collect(
  agg(
    groupBy(test, "type"),
    count(test$type),
    count(test$type)/total_test)
)

model = spark.lm(data = train, size ~ speed)
results = summary(model)

head(predict(model, test))

results$numFeatures

predictions = predict(model, train)
collect(predictions)

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


# # Save and then load a fitted MLlib model
# model_path = "../models/lm_model_particles"
# write.ml(model, model_path)
# model_loaded = read.ml(model_path)
# 
# # Check model summary
# summary(model)
# summary(model_loaded)
# 
# # Check model prediction
# model_loaded_pred = predict(model_loaded, test)
# head(model_loaded_pred)
