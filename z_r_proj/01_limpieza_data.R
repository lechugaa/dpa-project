library(tidyverse)


dta <- read_csv('data/Food_Inspections.csv')

names(dta) <-  dta %>% names() %>% tolower() %>% str_remove_all(" #") %>%  str_replace_all(" ", "_")

dta %>% distinct(results)

dta <- dta %>% 
  mutate_at(vars(results), ~ifelse(str_detect(., 'Pass'), 'Pass', 'Not Pass')) %>% 
  select(inspection_id, facility_type, risk, zip, inspection_date,
         inspection_type, results, violations, latitude, longitude)

#results ~ facility_type, risk, zip, 

#Change data types for zip and inspection_date
dta <- dta %>% 
  mutate_at(vars(zip), ~as.character(.)) %>% 
  mutate_at(vars(inspection_date), ~as.Date(., format = '%m/%d/%Y'))

#Clean NA

# dta %>% 
#   summary()
# 
# dta %>%
#   distinct(facility_type) %>% 
#   arrange(facility_type) %>% 
#   print(n= 999)


dta <- dta %>% 
  mutate_at(vars(facility_type), ~tolower(.)) %>%
  mutate_at(vars(facility_type), ~str_remove_all(., "\\'|\\-|\\(|\\)")) %>%
  mutate_at(vars(facility_type), ~str_replace_all(., "\\/", " ")) %>%
  mutate_at(vars(facility_type), ~str_replace_all(., 'restuarant', 'restaurant')) %>% 
  mutate_at(vars(facility_type), ~str_replace_all(., 'theatre', 'theater')) %>% 
  mutate_at(vars(facility_type), ~str_replace_all(., 'herabal', 'herbal')) %>% 
  mutate_at(vars(facility_type), ~str_replace_all(., 'day care', 'daycare')) %>% 
  mutate_at(vars(facility_type), ~str_replace_all(., 'long term', 'longterm')) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'childern|children|1023|5 years old'),
                                         yes =  'children service facility', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'conv|mart|gas station store'),
                                         yes =  'convenience store', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'assis|longterm|nursing|supportive'),
                                         yes =  'assisted living', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'herbal life|herbalife'),
                                         yes =  'herbalife', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'after school'),
                                         yes =  'after school', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'tavern|pub|brew|wine tasting|bar grill|hooka'),
                                         yes =  'bar', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'bakery'),
                                         yes =  'bakery', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'mobil'),
                                         yes =  'mobile food', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'kitchen'),
                                         yes =  'kitchen', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'restaurant|rstaurant|diner'),
                                         yes =  'restaurant', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'retail'),
                                         yes =  'retail', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'roof'),
                                         yes =  'rooftop', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'grocery'),
                                         yes =  'grocery store', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'liquor'),
                                         yes =  'liquor', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'popup'),
                                         yes =  'popup establishment', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'school|college|shcool'),
                                         yes =  'school', no = .)) %>%
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'daycare'),
                                         yes =  'daycare', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'cafeteria|coffee|cafe'),
                                         yes =  'coffee', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'drug store|pharmacy'),
                                         yes =  'drug store', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'gym|fitness|weight loss|exercise'),
                                         yes =  'gym', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'commissary|machine'),
                                         yes =  'vending machine', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'ice cream|paleteria|gelato'),
                                         yes =  'ice cream', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'banquet'),
                                         yes =  'banquet', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'lounge'),
                                         yes =  'lounge', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'church|religious'),
                                         yes =  'church', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'kiosk'),
                                         yes =  'kiosk', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'health|rehab'),
                                         yes =  'health', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'event'),
                                         yes =  'events', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'donut|hotdog|hot dog|popcorn|juice|tea|dessert|deli|salad|snack|candy|shake|watermelon|smoothie|food|sushi'),
                                         yes =  'other food', no = .)) %>% 
  mutate_at(vars(facility_type), ~ifelse(str_detect(., 'poultry|butcher|slaughter|meat'),
                                         yes =  'butcher', no = .)) %>% 
  mutate_at(vars(facility_type), ~replace_na(., "not specified"))

dta <- dta %>% 
  # distinct(inspection_type) %>% 
  # arrange(inspection_type) %>% 
  mutate_at(vars(inspection_type), ~tolower(.)) %>% 
  mutate_at(vars(inspection_type), ~ifelse(str_detect(., 'license'),
                                         yes =  'license', no = .)) %>% 
  mutate_at(vars(inspection_type), ~ifelse(str_detect(., 'task force|taskforce'),
                                         yes =  'task force', no = .)) %>% 
  mutate_at(vars(inspection_type), ~ifelse(str_detect(., 'canvass|canvas'),
                                         yes =  'canvass', no = .)) %>% 
  mutate_at(vars(inspection_type), ~ifelse(str_detect(., 'complaint'),
                                         yes =  'complaint', no = .)) 

cuenta_chrs <- function(text_val, chr = "|", escape = TRUE){

  # browser()
  if  (escape == TRUE) escape_chr = paste0('\\', chr)
  else escape_chr = chr
  
  
  if (is.na(text_val)) out <-  -1
  else {
  out <- text_val %>% 
    str_extract_all(escape_chr) %>% 
    .[[1]] %>% 
    length()
  
  }
  
  out
}


  
vec_cuenta_chrs = Vectorize(cuenta_chrs)

dta <- dta %>% 
  # filter(!is.na(violations)) %>% 
  mutate(num_violations = vec_cuenta_chrs(violations, '|')+1) %>% 
  select(-violations)

dta %>% 
  saveRDS('z_r_proj/dta_clean.rds')

dta %>% 
  count(num_violations) %>% 
  arrange(-n)
