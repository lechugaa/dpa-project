# library(stringr)
library(tidymodels)


dta <- readRDS('z_r_proj/dta_clean.rds') %>% 
  drop_na()

rcp <- recipe(results ~ ., data = dta) %>%
  update_role(inspection_id, inspection_date, new_role = 'ID') %>% 
  # update_role(violations, inspection_date, new_role = 'trash') %>% 
  step_date(inspection_date, features = c('dow', 'month')) %>% 
  step_rm(inspection_date) %>% 
  step_other(facility_type, threshold = 0.0002) %>% 
  step_other(inspection_type, threshold = 0.0002) %>% 
  step_string2factor(facility_type, zip, inspection_type, results) %>% 
  step_string2factor(risk, ordered = T)

rf_mod <- rand_forest(mode = 'classification', trees = 1000) %>% 
  set_engine('ranger', importance = 'impurity', num.threads = 8)



wflow <- workflow() %>% 
  add_model(rf_mod) %>% 
  add_recipe(rcp)


mod_fit <- wflow %>% 
  fit(data = dta)

mod_fit %>% 
  saveRDS('z_r_proj/mod_fit_factor.rds')

fit_obj <- pull_workflow_fit(mod_fit)$fit

fit_obj$variable.importance %>% 
  tidy() %>% 
  arrange(-x) %>% 
  mutate(imp_rel = x/sum(x))

fit_obj$predictions
