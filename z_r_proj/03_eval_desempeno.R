library(tidyverse)
library(tidymodels)

dta <- readRDS('z_r_proj/dta_clean.rds') %>% 
  drop_na()

mod_fit <- readRDS('z_r_proj/mod_fit_factor.rds')

fit_obj <- pull_workflow_fit(mod_fit)$fit

fit_obj$variable.importance %>% 
  tidy() %>% 
  arrange(-x) %>% 
  mutate(imp_rel = x/sum(x))

fit_obj$prediction.error

fit_obj$importance.mode


fit_obj$predictions %>% 
  as_tibble()

pred_tib <- mod_fit %>% 
  predict(dta %>% 
            head(1000)) %>% 
  bind_cols(dta %>% 
  head(1000))

pred_tib %>% 
  select(.pred_class, results) #%>% 
  count(.pred_class, results)


metrics_at_k <-  function(thresh = 0.05){

out <- fit_obj$predictions %>% 
  as_tibble() %>% 
  select(prob_pass = Pass) %>% 
  mutate(pred = ifelse(prob_pass >= thresh, "Pass", "Not Pass"),
         obs = dta$results) %>% 
  mutate(result = case_when(
    obs == 'Not Pass' & pred == 'Not Pass' ~ 'TN',
    obs == 'Pass' & pred == 'Pass' ~ 'TP',
    obs == 'Not Pass' & pred == 'Pass' ~ 'FP',
    obs == 'Pass' & pred == 'Not Pass' ~ 'FN',
    TRUE ~ 'Other'
  )) %>% 
  count(result) %>% 
  pivot_wider(names_from = 'result', values_from = 'n') %>% 
  mutate(precision = TP/(TP+FP),
         recall = TP/(FN+TP),
         accuracy = (TP + TN)/(TP+TN+FP+FN),
         specificity = TN /(TN + FP)) %>% 
  select(precision, recall, accuracy, specificity) %>% 
  mutate(thresh = thresh)

out

}

res <- seq(.01, .99, .01) %>% 
  map_dfr(metrics_at_k)

res

res %>% 
  pivot_longer(names_to = 'var', values_to = 'val', cols = -thresh) %>% 
  # filter(var != 'accuracy') %>% 
  ggplot(aes(thresh, val, color = var))+
  geom_line()

res %>% 
  filter(abs(recall - specificity) <= 0.011)

res %>% 
  mutate(fpr = 1 -specificity) %>% 
  select(thresh,recall, fpr) %>% 
  ggplot(aes(fpr, recall))+
  geom_line()+
  geom_abline(lty = 3)+
  coord_equal()+
  xlim(c(0,1))+
  ylim(c(0,1))

thresh = 0.69

obs_pred_tib <- fit_obj$predictions %>% 
  as_tibble() %>%
  mutate(truth = dta$results) %>% 
  mutate(predicted =  ifelse(Pass >= thresh, "Pass", "Not Pass")) %>% 
  mutate_at(vars(predicted, truth), ~factor(., levels = c('Pass', 'Not Pass'))) %>% 
  select(truth, Pass, NotPass = `Not Pass`, predicted)

obs_pred_tib %>% 
  count(truth, predicted) %>% 
  mutate(pct = n/sum(n)) %>% 
  select(-n) %>% 
  pivot_wider(names_from = 'truth', values_from = 'pct')

roc_auc(obs_pred_tib, truth = truth, Pass)

roc_curve(obs_pred_tib, truth = truth, Pass) %>% 
  autoplot()

roc_curve(obs_pred_tib, truth = truth, Pass) %>%
  ggplot(aes(1-specificity, sensitivity, color = .threshold))+
  geom_line()+
  geom_abline(lty  = 3)+
  coord_equal()

