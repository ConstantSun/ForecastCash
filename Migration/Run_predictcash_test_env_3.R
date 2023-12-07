library("tidyquant")
library("h2o")
library("timetk")
library("dplyr")
library("magrittr")
library("readxl")
library("lubridate")
library("tibble")

load("/opt/ml/processing/input/atm3105.RData")
atm <- atm1

holicode <- read_excel("/opt/ml/processing/input/HOLIDAYCODE_MOI.xlsx") 

holicode$datadate <- as.Date(holicode$datadate, tz = 'UTC')

holicode$HOLIDAYcode <- as.factor(holicode$HOLIDAYcode)

atm$datadate <- as.Date(atm$datadate+1)

atm$datadate <- atm$datadate +1

atm$ATMID <- as.factor(atm$ATMID)


atm <- atm[,c("ATMID","datadate","sum_cashout")]

atm <- atm[order(atm$datadate),]

atm <- atm[order(atm$ATMID),]
start_time <- Sys.time()
start_time


atm_ids <-  Sys.getenv("SIR")
# "4,5,6"
atm_ids <- as.list(strsplit(atm_ids, ",")[[1]])
print(typeof(atm_ids))

# for (k in c('990035'))
# {
for (k in atm_ids) 
{
  print(k)    # k type: character

  atmk <- atm %>%  filter(ATMID == k) 
  atmk$sum_cashout[is.na(atmk$sum_cashout)] <- median(atmk$sum_cashout, na.rm = T)

  datadate <- seq(max(atmk$datadate) + 1, max(atmk$datadate) + 7 , by = "1 day")

  forecast_add <- data.frame(ATMID = rep(k,7), datadate = 
          seq(max(atmk$datadate) + 1, max(atmk$datadate) + 7 , by = "1 day"),
          sum_cashout = rep(-1,7))


  atmk1 <- rbind(atmk,forecast_add)

  atmk1 <- atmk1 %>% tk_augment_timeseries_signature() 

  atmkclean <- atmk1 %>% mutate_if(is.ordered, ~ as.character(.) %>% as.factor)

  atmkclean <- dplyr::left_join(atmkclean, holicode, by = "datadate") 

  atmkclean$HOLIDAYcode <- as.factor(atmkclean$HOLIDAYcode)

  atmkclean$HOLIDAYcode  <- factor(atmkclean$HOLIDAYcode , levels=c(levels(atmkclean$HOLIDAYcode ), 88))

  atmkclean$HOLIDAYcode [is.na(atmkclean$HOLIDAYcode )] = 88

  atmkclean$HOLIDAYcode <- as.factor(atmkclean$HOLIDAYcode)

  atmkclean <- atmkclean %>% dplyr::mutate(t6_trngayle = ifelse(atmkclean$HOLIDAYcode %in% "88" & atmkclean$wday %in% "6","Y","N"))


  atmkclean <- atmkclean %>% tk_augment_lags(.data = .,
                                            .value = sum_cashout,
                                            .lags = 7:14) %>% filter(!is.na(sum_cashout_lag14))
    

  train_tbl <- atmkclean %>% filter(datadate < max(datadate) - 48)
  test_tbl <- atmkclean %>%  filter((datadate >= max(datadate) - 48) & (datadate < max(datadate)-13))
  
  train_tbl <- train_tbl 
  test_tbl <- test_tbl 
  
  forecast_tbl <- atmkclean %>%  filter((datadate >= max(datadate) - 13) & (datadate <= max(datadate)-7))
  
  y <- 'sum_cashout'
  x <- names(train_tbl) %>% setdiff(c('sum_cashout','ATMID','datadate'))
  
  h2o.init(max_mem_size = "140G")
  h2o.removeAll()
  
  train_h2o <- as.h2o(train_tbl)
  test_h2o <- as.h2o(test_tbl)
  
  forecast_h2o <- as.h2o(forecast_tbl)
  
  h2o.no_progress()
  
  
  set.seed(12787)
  automl_models_h2o <- h2o.automl(
    x = x, 
    y = y, 
    training_frame = train_h2o, 
    leaderboard_frame = test_h2o, 
    max_runtime_secs = 500,
    stopping_metric = "MAE")
  
  automl_leader <- automl_models_h2o@leader
  
  #Generate prediction using h2o.predict()
  pred_h2o <- h2o.predict(automl_leader, newdata = forecast_h2o)
  
  pred_h2o <- as.data.frame(pred_h2o) 
  
  
    
  #Investigate error
  error_tbl <- atmkclean %>% 
    filter(datadate >= max(datadate) - 13 & datadate <= max(datadate)-7) %>% 
    add_column(predict = pred_h2o$predict %>% as.vector()) %>%
    rename(actual = sum_cashout) %>%
    mutate(error = actual - predict,
      error_pct = error/actual) %>% 
    select(datadate, actual, predict, error, error_pct)

  error_tbl <- as.data.frame(error_tbl)
  error_tbl$id <- k

  fileConn<-file("/opt/ml/processing/output/2008_ATMerror_tbl.csv")
  write.csv(error_tbl, file =  paste('/opt/ml/processing/output/2008_ATMerror_tbl',k,'.csv',sep = "" ))


  #Error function
  f_error <- function(data){
    data %>% summarise(
      n=length(error),
      mean = mean(error),
      var = sum((error-mean)^2)/(n-1),
      std = sqrt(var),
      mae = mean(abs(error)),
      rmse = mean(error^2)^0.5,
      mape = mean(abs(error_pct)),
      mpe = mean(error_pct),
      skew = sum(((error - mean)/std)^3)/n,
      kurtosis = sum(((error - mean)/std)^4)/n-3,
      max_error = max(error),
      min_error = min(error)
    ) 
  }

  error_summary <-  error_tbl %>% f_error()
  error_summary <- as.data.frame(error_summary)
  error_summary$id <- k

  fileConn<-file("/opt/ml/processing/output/2008_ATMerror_summary.csv")
  write.csv(error_summary, file =  paste('/opt/ml/processing/output/2008_ATMerror_summary',k,'.csv',sep = "" ))

}

end_time <- Sys.time()
end_time - start_time
