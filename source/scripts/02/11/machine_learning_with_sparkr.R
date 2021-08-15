SPARK_HOME = "D:/Spark"
Sys.setenv(SPARK_HOME=SPARK_HOME)
library(SparkR,
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "local[*]",
               sparkConfig = list(spark.driver.memory = "4g"))

data = read.df(
  path = "../data/particles.csv", 
  source = "csv",
  delimiter = ",",
  inferSchema = "true",
  header = TRUE
)

head(data)
printSchema(data)

data = dropna(data)
data_ = data

df_list = randomSplit(data, c(7,3))
df_list

train = df_list[[1]]
test = df_list[[2]]


model = spark.lm(data = train, size ~ speed)
summary(model)

head(predict(model, test))

result$numFeatures

predictions = predict(model, train)
collect(predictions)

y_avg = collect(agg(predictions, y_avg = mean(y)))$y_avg
df = transform(predictions, 
               y_hat = predictions$prediction, 
               sq_res = (predictions$label - predictions$prediction)^2, 
               sq_tot = (y - y_avg)^2, 
               res = y - predictions$prediction)

df$prediction <- NULL

head(select(df, "y", "y_hat", "sq_res", "sq_tot"))
SSR = collect(agg(df, SSR = sum(df$sq_res)))
SST = collect(agg(df, SST = sum(df$sq_tot)))

Rsq = 1-(SSR[[1]]/SST[[1]])
p = 10
N = nrow(df)
aRsq = Rsq - (1 - Rsq)*((p - 1)/(N - p))


# Save and then load a fitted MLlib model
model_path = "../models/lm_model_particles"
write.ml(model, model_path)
model_loaded = read.ml(model_path)

# Check model summary
summary(model)
summary(model_loaded)

# Check model prediction
model_loaded_pred = predict(model_loaded, test)
head(model_loaded_pred)

