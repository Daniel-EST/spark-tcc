SPARK_HOME = "D:/Spark"

Sys.setenv(SPARK_HOME=SPARK_HOME)


library(ggplot2)
library(SparkR,
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "local[*]",
               sparkConfig = list(spark.driver.memory = "1g"))

data = read.df(
  path = "../../../data/used_cars_data.csv",
  source = "csv",
  delimiter = ",",
  inferSchema = "true",
  na.strings = "None",
  header = TRUE
)

printSchema(data)
nrow(data)
ncol(data)

data = drop(data, 
            c("vin",
              "sp_name",
              "sp_id",
              "daysonmarket",
              "description", 
              "franchise_dealer",
              "franchise_make",
              "listed_date",
              "listing_id",
              "main_picture_url",
              "major_options",
              "latitude",
              "longitude",
              "trimId",
              "trim_name",
              "dealer_zip",
              "interior_color",
              "exterior_color",
              "model_name",
              "engine_cylinders",
              "transmission_display",
              "wheel_system_display",
              "savings_amount",
              "salvage",
              "theft_title"))


data = filter(data, isNotNull(data$price))
data = filter(data, data$year > 1990 & data$year <= 2021)


cols = lapply(columns(data), \(x) alias(count(data[[x]]), x))

notnull_count = collect(select(data,  cols))
n = nrow(data)

notnull_prop = (notnull_count/n)

cols = colnames(notnull_prop)[notnull_prop > 0.5]
data = select(data, cols)

convert_col = c("back_legroom", 
                "front_legroom", 
                "fuel_tank_volume", 
                "height",
                "length", 
                "maximum_seating", 
                "wheelbase",
                "width")

for(col in convert_col){
  data = withColumn(data, col, regexp_replace(data[[col]], " \\w+", ""))
  data[[col]] = cast(data[[col]], "double")
}

data$price = cast(data$price, "double")
data$back_legroom = cast(data$back_legroom, "double")
data$city_fuel_economy = cast(data$city_fuel_economy, "double")
data$engine_displacement = cast(data$engine_displacement, "double")
data$horsepower = cast(data$horsepower, "double")
data$mileage = cast(data$mileage, "double")
data$seller_rating = cast(data$seller_rating, "double")
data$highway_fuel_economy = cast(data$highway_fuel_economy, "double")

data = withColumn(data, "power_rpm", regexp_extract(data[["power"]], "\\d+,\\d+", 0))
data = withColumn(data, "power_rpm", regexp_replace(data[["power_rpm"]], ",", ""))
data$power_rpm = cast(data$power_rpm, "double")

data = drop(data, "power")

data = withColumn(data, "torque_power", regexp_extract(data[["torque"]], "\\d+", 0))
data$torque_power = cast(data$torque_power, "double")
data = withColumn(data, "torque_rpm",  regexp_extract(data[["torque"]], "\\d+,\\d+", 0))
data = withColumn(data, "torque_rpm", regexp_replace(data[["torque_rpm"]], ",", ""))
data$torque_rpm = cast(data$torque_rpm, "double")

data = drop(data, "torque")

data$year = cast(data$year, "integer")
data$owner_count = cast(data$owner_count, "integer")
data$maximum_seating = cast(data$maximum_seating, "integer")

plot_histogram = function(data, colname, nbins = 32){
  hist = histogram(data, data[[colname]], nbins = nbins)
  
  p = ggplot(hist, aes(x = centroids, y = counts)) +
    geom_bar(stat = "identity", col = "black") +
    ggtitle("") + 
    xlab(colname) + ylab("Frequência") +
    theme_minimal()
  
  print(p)
  
  return(list(colname, hist))
}

categoric = c()
numeric = c()
for(col in dtypes(data)){
  if(col[[2]] != "double"){
    categoric = c(categoric, col[[1]])
  } else {
    numeric = c(numeric, col[[1]])
  }
}

means = lapply(numeric, \(x) alias(avg(data[[x]]), x))
means = collect(select(data, means))

infinity = lapply(columns(data), \(x) alias(ifelse(lit(data[[x]] == "Inf"), NA, data[[x]]), x))
data = select(data, infinity)

variances = lapply(numeric, \(x) alias(var(data[[x]],  na.rm = FALSE),  x))
variances = collect(select(data, variances))

options(scipen = 999)
col_hist = c("city_fuel_economy", "highway_fuel_economy", "mileage")
histograms = lapply(col_hist, \(x) plot_histogram(data, x, nbins = 32))

collect(
  agg(data,
      max_price = max(data$price),
      min_price = min(data$price),
      max_maximum_seating = max(data$maximum_seating),
      min_maximum_seating= min(data$maximum_seating),
      max_owner_count = max(data$owner_count),
      min_owner_count = min(data$owner_count),
      max_seller_rating = max(data$seller_rating),
      min_seller_rating = min(data$seller_rating))
)

data = transform(data, price = ifelse(data$price <= 0, NA, data$price))
data = filter(data, isNotNull(data$price))
data = transform(data, listing_color = ifelse(data$listing_color == "UNKNOWN", NA, data$listing_color))

boxplot_price = collect(
  agg(data,
      med = percentile_approx(data$price, percentage=.5),
      q1 = percentile_approx(data$price, percentage=.25),
      q3 = percentile_approx(data$price, percentage=.75))
  )

ggplot(boxplot_price,
       aes(x = "",
           ymin = q1 - 1.5 * (q3 - q1),
           lower = q1,
           middle = med,
           upper = q3,
           ymax = q3 + 1.5 * (q3 - q1))) +
  geom_boxplot(stat = "identity")  +
  ggtitle("Boxplot do Preço") +
  ylab("Preço") + xlab("") +
  theme_minimal() 

medians = lapply(numeric, \(x) alias(percentile_approx(data[[x]], percentage = .5), x))
medians = collect(select(data, medians))

data = fillna(data, as.list(medians))

count = lapply(categoric, \(x) {
  arrange(
    agg(
      groupBy(
        filter(data, isNotNull(data[[x]])), alias(data[[x]], x)), alias(count(data[[x]]), "n")
      ), col = "n", decreasing = TRUE)
  })

count = lapply(count, \(x) collect(x)) |> 
  `names<-`(categoric)

pareto = function(dados){
  dados$prop = dados$n/sum(dados$n)
  dados$cumsum_ = cumsum(dados$n)
  dados$dados = ordered(dados[,1], dados[,1])
  
  label = sprintf("%.0f%%", 100 * dados$cumsum_ / sum(dados$n))
  
  ggplot(dados, aes(x = dados)) +
    geom_bar(aes(y = n), stat = "identity") +
    geom_point(aes(y = cumsum_)) +
    geom_path(aes(y = cumsum_, group = 1)) +
    geom_text(aes(y = cumsum_), label = label, vjust = -1.7) +
    scale_y_continuous("Frequência",
                       sec.axis = sec_axis(
                         ~ . / sum(dados$n),
                         labels = scales::percent,
                         name = "Acumulado"
                       )) +
    coord_cartesian(clip = 'off') +
    theme_minimal() +
    theme(plot.margin = margin(100, 10, 10, 10, "pt"),
          axis.title.y.left = element_text("Frequência"),
          axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1.25),
          axis.title.x.bottom = element_blank())
}

pareto(head(count$body_type, 9))
data = transform(data, body_type = ifelse(data$body_type %in% {{head(count$body_type, 9)[, 1]}}, data$body_type, NA))

city_95 = count$city$city[cumsum(count$city$n)/sum(count$city$n) <= 0.95]
data = transform(data, city = ifelse(data$city %in% {{city_95}}, data$city, "Others"))

engine_type_95 = count$engine_type$engine_type[cumsum(count$engine_type$n)/sum(count$engine_type$n) <= 0.95]
data = transform(data, engine_type = ifelse(data$engine_type %in% {{engine_type_95}}, data$engine_type, "Others"))

fuel_type_95 = count$fuel_type$fuel_type[cumsum(count$fuel_type$n)/sum(count$fuel_type$n) <= 0.95]
data = transform(data, fuel_type = ifelse(data$fuel_type %in% {{fuel_type_95}}, data$fuel_type, "Others"))

listing_color_95 = count$listing_color$listing_color[cumsum(count$listing_color$n)/sum(count$listing_color$n) <= 0.95]
data = transform(data, listing_color = ifelse(data$listing_color %in% {{listing_color_95}}, data$listing_color, "Others"))

make_name_95 = count$make_name$make_name[cumsum(count$make_name$n)/sum(count$make_name$n) <= 0.95]
data = transform(data, make_name = ifelse(data$make_name %in% {{make_name_95}}, data$make_name, "Others"))

data = transform(data, transmission = ifelse(data$transmission %in% c("A", "CVT", "M", "Dual Clutch"), data$transmission, NA))

data = transform(data, wheel_system = ifelse(data$wheel_system %in% c("FWD", "AWD", "4WD", "4X2"), data$wheel_system, NA))

data = transform(data, year = ifelse(data$year > 2021, NA, data$year))

data = transform(data, fleet = ifelse(data$fleet %in% c("True", "False"), data$fleet, NA))
data = transform(data, has_accidents = ifelse(data$has_accidents %in% c("True", "False"), data$has_accidents, NA))
data = transform(data, isCab = ifelse(data$isCab %in% c("True", "False"), data$isCab, NA))
data = transform(data, is_new = ifelse(data$is_new %in% c("True", "False"), data$is_new, NA))

ggplot(count$maximum_seating, aes(x = maximum_seating)) +
  geom_bar(aes(y = n), stat = "identity") +
  geom_text(aes(y = n, label = n), stat = "identity") +
  theme_minimal() +
  scale_x_continuous(limits = c(2, 15)) +
  theme(plot.margin = margin(100, 10, 10, 10, "pt"),
        axis.title.y.left = element_text("Frequência"),
        axis.title.x.bottom = element_blank())

ggplot(count$year, aes(x = year)) +
  geom_bar(aes(y = n), stat = "identity") +
  theme_minimal() +
  theme(plot.margin = margin(100, 10, 10, 10, "pt"),
        axis.title.y.left = element_text("Frequência"),
        axis.title.x.bottom = element_blank())


most_frequent = lapply(count, \(x) x[1,1]) |> 
  `names<-`(categoric)

data = fillna(data, most_frequent)

data = drop(data, data$frame_damaged)

train_test_split = randomSplit(data, c(75, 25), 20210908)

train = train_test_split[[1]]
test = train_test_split[[2]]

nrow(train)
nrow(test)

model = SparkR::spark.svmLinear(train, price ~ .)
summary(model)

predictions = predict(model, test)
head(predictions)

y_avg = collect(agg(predictions, avg(predictions$label)))[[1]]
df = 
  agg(predictions,
      sq_res = sum((predictions$label - predictions$prediction)^2), 
      sq_tot = sum((predictions$label - y_avg)^2)) 

SSR = collect(select(df, df$sq_res))[[1]]
SST = collect(select(df, df$sq_tot))[[1]]

Rsq = 1 - (SSR/SST)
sprintf(" R : %.3f", Rsq)

write.ml(model, "./model/svm_cars")

model = read.ml("./model/svm_cars")