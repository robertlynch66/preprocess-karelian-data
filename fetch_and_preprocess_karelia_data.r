initialize_libraries <- function() {
  library(dplyr)
  library(tidyr)
  library(rethinking)
  library(DBI)
  library(RPostgres)
  library(googlesheets)
}

establish_database_connection <- function(my_dbname="learning-from-our-past",
                                          my_host="karelia-17.it.helsinki.fi", 
                                          my_port=5432, 
                                          my_user="robert") {
  db_connection <- dbConnect(RPostgres::Postgres(),
                             dbname=my_dbname, host=my_host, 
                             port=my_port, user=my_user)
  
  return(db_connection)
}

load_pops_from_googlesheets <- function() {
  # Read in the csv files populations with social status
  
  # get the populations google sheet
  pops <- gs_title("Location populations")
  
  # get the sheet
  pops <- gs_read(ss=pops, ws = "Sheet1", header=TRUE)
  
  # convert to data.frame
  pops <- as.data.frame(pops)
  pops <- arrange(pops,placeId)
  
  # clean the google sheets docs
  names(pops) <- c("DONT_USE", "frequency", "name","region",
                   "name2", "population","latitude","longitude")
  #pops <- select(pops,1:4,6:8)
  return(pops)
}

load_jobs_from_googlesheets <- function() {
  # Read in the csv files populations and occupations with social status
  
  # Get the professions google sheet
  jobs <- gs_title("Occupations")
  
  # Get the sheet
  jobs <- gs_read(ss=jobs, ws = "Sheet1", header=TRUE)
  
  # convert to data.frame
  jobs <- as.data.frame(jobs)
  
  # clean the google sheets docs
  names(jobs) <- c("profession","frequency","profession_EN","agriculture",
                   "man_labor","education","statistics_Finland","1950_census","social_class")
  return(jobs)
}

fix_encoding_in_place_table <- function(place) {
  # encode the variables correctly for the place table
  # make an empty vector
  b <- vector(mode="character", length=length(place$name))
  # do a for loop that fills in the vector 'b' with the character conversions
  for (i in 1:length(place$name)) {
    b[i] <- iconv(place[i, 2], from="UTF-8", to="latin1")
  }
  # convert the column in the dataframe to the new vector
  place$name <- b
  
  c <- vector(mode="character", length=length(place$stemmedName))
  # do a for loop that fills in the vector 'b' with the character conversions
  for (i in 1:length(place$stemmedname)) {
    c[i] <- iconv(place[i, 4], from="UTF-8", to="UTF-8")
  }
  # convert the column in the dataframe to the new vector
  place$stemmedname <- c
  
  d <- vector(mode="character", length=length(place$extractedname))
  # do a for loop that fills in the vector 'b' with the character conversions
  for (i in 1:length(place$extractedname)) {
    d[i] <- iconv(place[i, 5], from="UTF-8", to="UTF-8")
  }
  # convert the column in the dataframe to the new vector
  place$extractedname <- d
  
  return(place)
}

fix_encoding_in_profession_table <- function(profession) {
  e <- vector(mode="character", length=length(profession$name))
  
  # do a for loop that fills in the vector 'b' with the character conversions
  for (i in 1:length(profession$name)) {
    e[i] <- iconv(profession[i,2], from="UTF-8", to="UTF-8")
  }
  
  # convert the column in the dataframe to the new vector
  profession$name <- e
  return(profession)
}

add_spouse_ids_to_person_table <- function(person, marriage) {
  # find all married men and join them to their wives
  person <- person %>%
    left_join(marriage, by = c("id"="primaryId")) %>%
    left_join(marriage, by = c("id"="spouseId")) 
  
  
  person$weddingYear = person$weddingYear.x  # your new merged column start with x
  person$weddingYear[!is.na(person$weddingYear.y)] = person$weddingYear.y[!is.na(person$weddingYear.y)] # merge with y
  person$weddingYear <- ifelse(person$weddingYear < 1970, person$weddingYear, person$weddingYear-100)
  drops <- c("weddingYear.x","weddingYear.y")
  person <- drop_columns_from_table(person,drops)
  
  person$spouse_id = person$spouseId  # your new merged column start with x
  person$spouse_id[!is.na(person$primaryId)] = person$primaryId[!is.na(person$primaryId)]  # merge with y
  # drops <- c("primaryId","spouseId")
  # person <- drop_columns_from_table(person,drops)
  return(person)
}

add_number_of_children_to_person_table <- function(person, child) {
  # add number of kids for dads
  child2 <- child %>%
    group_by(primaryParentId) %>%
    summarise(primarys_kids=n()) 
  # add number of kids for moms
  child3 <- child %>%
    group_by(spouseParentId) %>%
    summarise(spouses_kids=n())
  
  # row_bind the two tables
  num_kids <- bind_rows(child2,child3)
  #combine the columns kids
  num_kids$kids <- num_kids$primarys_kids
  num_kids$kids[is.na(num_kids$kids)] <- num_kids$spouses_kids[is.na(num_kids$kids)]
  #combine the columns mother and father ids
  num_kids$id <- num_kids$primaryParentId
  num_kids$id[is.na(num_kids$id)] <- num_kids$spouseParentId[is.na(num_kids$id)]
  #delete redundant columns
  num_kids <- num_kids %>% select ("kids", "id") %>%
    #sort by id
    arrange(id)
  
  # Now num_kids is a table with parent ids and their number of kids
  # join this to the person2 table and name is person3
  person <- left_join(person, num_kids, by= "id")
  
  # replace NA's in kids column with 0
  person$kids[is.na(person$kids)] <- 0
  
  person <- person%>%arrange(id)
  
  return(person)
}

add_birthplace_data_to_person_table <- function(person, place) {
  person <- person %>%
    left_join(place, by=c("birthPlaceId"="place_id"))
  
  drops = c("sourceTextId", "ambiguousRegion", "frequency")
  person <- drop_columns_from_table(person, drops)
  person <- dplyr::rename(person, birthplace = name, birthregion = region,
                          birthpopulation = population, birthlat = lat, birthlon = lon)
  
  #link to spouse id and add spouse population and region
  
  person_trimmed <- person %>% select("birthYear", "professionId", "returnedKarelia",
                                      "weddingYear", "kids", "birthplace", "birthpopulation",
                                      "birthregion", "birthlat", "birthlon", "spouse_id")
  person <- person %>%
    left_join(person_trimmed, by=c("id"="spouse_id"))
  
  person <- dplyr::rename(person,
                          birthYear = birthYear.x, professionId = professionId.x,
                          returnedKarelia = returnedKarelia.x, weddingyear = weddingYear.x,
                          kids = kids.x, birthplace = birthplace.x, birthpopulation = birthpopulation.x,
                          birthregion = birthregion.x, birthlat = birthlat.x, birthlon = birthlon.x,
                          birthYear_spouse = birthYear.y, professionId_spouse = professionId.y,
                          returnedKarelia_spouse = returnedKarelia.y, weddingyear_spouse = weddingYear.y,
                          kids_spouse = kids.y, birthplace_spouse = birthplace.y,
                          birthpopulation_spouse = birthpopulation.y, birthregion_spouse = birthregion.y,
                          birthlat_spouse = birthlat.y, birthlon_spouse = birthlon.y)
  
  return(person)
}

add_professions_to_person_table <- function(person, profession) {
  # add professions
  person <- person %>%
    left_join(profession, by=c("professionId"="profession_id"))
  
  person <- drop_columns_from_table(person, "frequency")
  person <- dplyr::rename(person, profession = name, profession_en = profession_EN)
  
  # add spouses profession (just social class)
  person_filt <- person %>% select("social_class", "education", "id", "spouse_id")
  person <- person %>%
    left_join(person_filt, by=c("id"="spouse_id"))
  drops <- "id.y"
  person <- drop_columns_from_table(person, drops)
  person <- dplyr::rename(person, education = education.x, social_class = social_class.x,
                          education_spouse = education.y, social_class_spouse = social_class.y)
  
  return(person)
}

add_livingrecord_data_to_person_table <- function(person, livingrecord) {
  # Get mean number of migrations 
  migrations <- livingrecord %>% 
    group_by(personId) %>%
    summarise(movesbefore1940=sum(movedOut<40,na.rm=TRUE),movesafter1945=sum(movedOut>44,na.rm=TRUE)) %>%
    arrange(desc(movesbefore1940))
  
  person <- person %>%
    left_join(migrations, by=c("id"="personId"))
  
  migrations <- livingrecord %>% 
    group_by(personId) %>%
    summarise(peacetime_migrations=sum(movedOut<39 | movedOut>45,na.rm=TRUE)) %>%
    arrange(desc(peacetime_migrations))
  
  person <- person %>%
    left_join(migrations, by=c("id"="personId"))
  
  migrations <- livingrecord %>% 
    group_by(personId) %>%
    summarise(total_migrations=n())
  
  person <- person %>%
    left_join(migrations, by=c("id"="personId"))
  
  return(person)
}

add_outbred_to_person_table <- function(person) {
  person <- person %>%
    mutate(outbred = ifelse (birthregion!=birthregion_spouse, 1, 0))
  
  return(person)
}

add_previous_marriages_flag <- function(person) {
  person <- person %>% 
    mutate(previous_marriage = ifelse (previousMarriages== "true", 1, 0))
  return(person)
}

add_sons_and_daughters_to_person_table <- function(person, child) {
  # add sons and daughters to m table
  # add number of kids for dads
  primary <- child %>%
    group_by(primaryParentId, sex) %>%
    summarise(primarys_kids=n()) %>% na.omit()
  # add number of kids for moms
  spouse <- child %>%
    group_by(spouseParentId, sex) %>%
    summarise(spouses_kids=n()) %>% na.omit()
  
  primary$daughters <- ifelse(primary$sex == "f", primary$primarys_kids, 0)
  primary$sons <- ifelse(primary$sex == "m", primary$primarys_kids, 0)
  primarysSons <- aggregate(primary$sons, by=list(primary$primaryParentId), FUN=sum)
  primarysDaughters <- aggregate(primary$daughters, by=list(primary$primaryParentId), FUN=sum)
  primarysSons <- dplyr::rename(primarysSons, primaryParentId = Group.1, sons = x)
  primarysDaughters <- dplyr::rename(primarysDaughters, primaryParentId = Group.1, daughters = x)
  primarysKids <- merge(primarysSons, primarysDaughters, by="primaryParentId")
  
  spouse$daughters <- ifelse(spouse$sex == "f", spouse$spouses_kids, 0)
  spouse$sons <- ifelse(spouse$sex == "m", spouse$spouses_kids, 0)
  spousesSons <- aggregate(spouse$sons, by=list(spouse$spouseParentId), FUN=sum)
  spousesDaughters <- aggregate(spouse$daughters, by=list(spouse$spouseParentId), FUN=sum)
  spousesSons <- dplyr::rename(spousesSons, spouseParentId = Group.1, sons = x)
  spousesDaughters <- dplyr::rename(spousesDaughters, spouseParentId = Group.1, daughters = x)
  spousesKids <- merge(spousesSons, spousesDaughters, by="spouseParentId")
  
  primarysKids <- dplyr::rename(primarysKids, id = primaryParentId)
  person <- left_join(person, primarysKids, by=c("id"="id"))
  spousesKids <- dplyr::rename(spousesKids, id = spouseParentId)
  person <- left_join(person, spousesKids, by=c("id"="id"))
  person$daughters <- ifelse(is.na(person$daughters.x), person$daughters.y, person$daughters.x)
  person$sons <- ifelse(is.na(person$sons.x), person$sons.y, person$sons.x)
  
  drops = c("sons.x", "sons.y", "daughters.x", "daughters.y")
  person <- drop_columns_from_table(person, drops)
  
  person$sons[is.na(person$sons)] <- 0
  person$daughters[is.na(person$daughters)] <- 0
  person <- person %>% arrange(id)
  
  return(person)
}

add_age_in_1970_and_at_marriage <- function(person) {
  person$age_1970 <- 1970 - person$birthYear
  person$age_1970 <- ifelse (person$age_1970 > 15 & person$age_1970 < 101, person$age_1970, NA)
  person <- person %>% mutate(age_at_marriage=weddingyear - birthYear)
  
  # clean the bad values for age at marriage
  person$age_at_marriage<- ifelse (person$age_at_marriage>15 & person$age_at_marriage<60, person$age_at_marriage, NA)
  return(person)
}

add_couple_id_to_person_table <- function(person) {
  # just add id and spouse id - these values are unique to each couple i'm pretty sure (e.g no duplicate couples).
  person$couple_id <- person$id + person$spouse_id
  
  # arrange them
  person <- person %>% arrange(couple_id)
  
  # then do that sequential labeling - we have to do this a second time in the model code because of missing cases
  person$couple_id_seq <- cumsum(c(1,as.numeric(diff(person$couple_id)) != 0))
  return(person)
}

add_hectares_to_person_table <- function(person, farms) {
  person<-person %>%
    
    left_join (farms, by = c("farmDetailsId"="id")) 
  
  drops <- c("dairyFarm","coldFarm",             
             "asutustila","maanhankintalaki",      
             "editLog","animalHusbandry") 
  person <- drop_columns_from_table(person, drops)
  return(person)
}

add_returned_to_karelia_for_spouses <- function(person) {
  person$ret_kar <- ifelse(!person$primaryPerson &
                             !is.na(person$spouse_id) &
                             (person$birthYear < 1913 | person$weddingyear < 1942),
                           person$returnedKarelia_spouse, person$returnedKarelia)
  
  person$ret_kar <- ifelse(person$primaryPerson,
                           person$returnedKarelia, person$ret_kar)
  
  person$returnedKarelia <- person$ret_kar
  person<- drop_columns_from_table(person, "ret_kar")
  return(person)
}


add_first_and_last_destinations_to_person_table <- function(person, pops, livingrecord, place) {
  
  livingrecord$merged <- ifelse (is.na(livingrecord$movedIn), livingrecord$movedOut, livingrecord$movedIn)
  livingrecord <- left_join(livingrecord, place, by = c("placeId" = "place_id"))
  
  merdata<-left_join(pops, livingrecord, by = c("name" = "name"))
  merdata$merKarelia <- with(merdata, ifelse(merged>39 & merged <45 & region.y == "karelia",1,0))
  merdata$merFinland <- with(merdata, ifelse(merged>38 & merged <42 & region.y == "other",1,0))
  
  merkar <- merdata[ which(merdata$merKarelia == 1), ]
  merkar2 <- merkar %>% select(c("personId", "longitude", "latitude", "population.x", "merged", "DONT_USE"))
  merkar3 <- merkar2 [order(merkar2$merged, decreasing = TRUE),]
  merkar4 <- merkar3[!duplicated (merkar3$personId),]
  
  merfin <- merdata[which(merdata$merFinland == 1),]
  merfin2 <- merfin %>% select(c("personId", "longitude", "latitude", "population.x", "merged", "DONT_USE"))
  merfin3 <- merfin2 [order(merfin2$merged, decreasing = TRUE),]
  merfin4 <- merfin3[!duplicated (merfin3$personId),]
  
  person <- left_join(person,merfin4, by = c("id" = "personId"))
  person <- left_join(person,merkar4, by = c("id" = "personId"))
  
  drops = c("DONT_USE.x", "DONT_USE.y", "merged.x", "merged.y")
  person <- drop_columns_from_table(person, drops)
  person <- dplyr::rename(person,
                          fdf_population = population.x.x, fdf_longitude = longitude.x, fdf_latitude = latitude.x,
                          rdk_population = population.x.y, rdk_longitude = longitude.y, rdk_latitude = latitude.y)
  
  return(person) 
}

add_total_peacetime_migrations <- function(person) {
  person$returnedKarelia <- gsub("true",1, person$returnedKarelia)
  person$returnedKarelia <- gsub( "false",0, person$returnedKarelia)
  person$returnedKarelia <- gsub( "unknown",NA, person$returnedKarelia)
  person$returnedKarelia <- as.integer(person$returnedKarelia)
  
  person$total_peacetime_migrations <- person$total_migrations-person$returnedKarelia
  return(person)
}

add_first_child_birth_year <- function(person, child) {
  # add children for mother ids
    child_spouse <- child %>%
      group_by (spouseParentId) %>%
      filter(birthYear == min(birthYear)) %>%
      group_by (spouseParentId) %>%
      filter(child_id==min(child_id))
    person2<- person %>%
      left_join (child_spouse , by= c("id"="spouseParentId")) %>%
      select ("id","birthYear.y") %>%
     dplyr::rename (spouses_first_child_YOB= birthYear.y)
    
  # add children for father ids
    child_primary <- child %>%
      group_by (primaryParentId) %>%
      filter (birthYear == min(birthYear)) %>%
      group_by (primaryParentId) %>%
        filter (child_id==min(child_id))
    person3<- person2 %>%
      left_join (child_primary , by =c("id"="primaryParentId")) %>%
      select ("id","spouses_first_child_YOB","birthYear") %>%
    dplyr::rename ( primaries_first_child_YOB= birthYear)
    
    
    person3$first_child_YOB <- ifelse(is.na(person3$spouses_first_child_YOB), 
                                      person3$primaries_first_child_YOB, person3$spouses_first_child_YOB)
    
    person4 <- person3 %>% select ("id","first_child_YOB")
    
    person<- person %>% left_join (person4, by=c("id"="id"))
    return(person)
}

add_last_child_birth_year <- function(person, child) {
  # add children for mother ids
  child_spouse <- child %>%
    group_by (spouseParentId) %>%
    filter(birthYear == max(birthYear)) %>%
    group_by (spouseParentId) %>%
    filter(child_id==max(child_id))
  person2<- person %>%
    left_join (child_spouse , by= c("id"="spouseParentId")) %>%
    select ("id","birthYear.y") %>%
    dplyr::rename (spouses_last_child_YOB= birthYear.y)
  
  # add children for father ids
  child_primary <- child %>%
    group_by (primaryParentId) %>%
    filter (birthYear == max(birthYear)) %>%
    group_by (primaryParentId) %>%
    filter (child_id==max(child_id))
  person3<- person2 %>%
    left_join (child_primary , by =c("id"="primaryParentId")) %>%
    select ("id","spouses_last_child_YOB","birthYear") %>%
    dplyr::rename ( primaries_last_child_YOB= birthYear)
  
  
  person3$last_child_YOB <- ifelse(is.na(person3$spouses_last_child_YOB), 
                                    person3$primaries_last_child_YOB, person3$spouses_last_child_YOB)
  
  person4 <- person3 %>% select ("id","last_child_YOB")
  
  person<- person %>% left_join (person4, by=c("id"="id"))
  return(person)
}

add_age_at_first_birth <- function(person) {
  person$age_at_first_birth<- person$first_child_YOB - person$birthYear
  return(person)
}

add_military_ranks_to_persons_table <- function(person, militaryranks) {
  militaryranks <- drop_columns_from_table(militaryranks, c("editLog")) %>% 
    dplyr::rename (militaryrank= name)
  person <- person %>% left_join (militaryranks, by= c("militaryRankId"="id")) 
  person <- drop_columns_from_table(person, c("militaryRankId"))

  person$militaryrank_english <- ifelse(person$militaryrank=="sotamies","private",
  ifelse(person$militaryrank=="matruusi","seaman",
  ifelse(person$militaryrank=="korpraali","corporal",
  ifelse(person$militaryrank=="ylimatruusi","higher seaman",
  ifelse(person$militaryrank=="alikersantti","lower sergeant",
  ifelse(person$militaryrank=="kersantti","sergeant",
  ifelse(person$militaryrank=="ylikersantti","higher sergeant",
  ifelse(person$militaryrank=="vääpeli","NCO",
  ifelse(person$militaryrank=="pursimies","petty officer",
  ifelse(person$militaryrank=="ylivääpeli","higher NCO",
  ifelse(person$militaryrank=="ylipursimies","higher petty officer",
  ifelse(person$militaryrank=="sotilasmestari","sergeant major",
  ifelse(person$militaryrank=="vänrikki","second lieutenant",
  ifelse(person$militaryrank=="aliluutnantti","lower lieutenant",
  ifelse(person$militaryrank=="luutnantti","lieutenant",
  ifelse(person$militaryrank=="yliluutnantti","higher lieutenant",
  ifelse(person$militaryrank=="kapteeni","captain",
  ifelse(person$militaryrank=="kapteeniluutnantti","captain-lieutenant",
  ifelse(person$militaryrank=="majuri","major",
  ifelse(person$militaryrank=="komentajakapteeni","commander-captain",
  ifelse(person$militaryrank=="everstiluutnantti","colonel-lieutenant",
  ifelse(person$militaryrank=="komentaja","commander",
  ifelse(person$militaryrank=="eversti","colonel",
  ifelse(person$militaryrank=="kommodori","commodore",
  ifelse(person$militaryrank=="kenraalimajuri","general-major",
  ifelse(person$militaryrank=="kenraaliluutnantti","general-lieutenant",
  ifelse(person$militaryrank=="kenraali","general",NA)))))))))))))))))))))))))))
  
  
  person$militaryrank_hierarchy <- ifelse(person$militaryrank=="sotamies",1,
  ifelse(person$militaryrank=="matruusi",1,
  ifelse(person$militaryrank=="korpraali",2,
  ifelse(person$militaryrank=="ylimatruusi",2,
  ifelse(person$militaryrank=="alikersantti",3,
  ifelse(person$militaryrank=="kersantti",4,
  ifelse(person$militaryrank=="ylikersantti",5,
  ifelse(person$militaryrank=="vääpeli",6,
  ifelse(person$militaryrank=="pursimies",6,
  ifelse(person$militaryrank=="ylivääpeli",7,
  ifelse(person$militaryrank=="ylipursimies",7,
  ifelse(person$militaryrank=="sotilasmestari",8,
  ifelse(person$militaryrank=="vänrikki",9,
  ifelse(person$militaryrank=="aliluutnantti",9,
  ifelse(person$militaryrank=="luutnantti",10,
  ifelse(person$militaryrank=="yliluutnantti",11,
  ifelse(person$militaryrank=="kapteeni",12,
  ifelse(person$militaryrank=="kapteeniluutnantti",12,
  ifelse(person$militaryrank=="majuri",13,
  ifelse(person$militaryrank=="komentajakapteeni",13,
  ifelse(person$militaryrank=="everstiluutnantti",14,
  ifelse(person$militaryrank=="komentaja",14,
  ifelse(person$militaryrank=="eversti",15,
  ifelse(person$militaryrank=="kommodori",15,
  ifelse(person$militaryrank=="kenraalimajuri",16,
  ifelse(person$militaryrank=="kenraaliluutnantti",17,
  ifelse(person$militaryrank=="kenraali",18, NA)))))))))))))))))))))))))))
  
  
  person$militaryrank_group <- ifelse(person$militaryrank=="sotamies","Crew",
  ifelse(person$militaryrank=="matruusi","Crew",
  ifelse(person$militaryrank=="korpraali","Crew",
  ifelse(person$militaryrank=="ylimatruusi","Crew",
  ifelse(person$militaryrank=="alikersantti","Non-commissioned officers",
  ifelse(person$militaryrank=="kersantti","Non-commissioned officers",
  ifelse(person$militaryrank=="ylikersantti","Non-commissioned officers",
  ifelse(person$militaryrank=="vääpeli","Non-commissioned officers",
  ifelse(person$militaryrank=="pursimies","Non-commissioned officers",
  ifelse(person$militaryrank=="ylivääpeli","Non-commissioned officers",
  ifelse(person$militaryrank=="ylipursimies","Non-commissioned officers",
  ifelse(person$militaryrank=="sotilasmestari","Non-commissioned officers",
  ifelse(person$militaryrank=="vänrikki","Company officer",
  ifelse(person$militaryrank=="aliluutnantti","Company officer",
  ifelse(person$militaryrank=="luutnantti","Company officer",
  ifelse(person$militaryrank=="yliluutnantti","Company officer",
  ifelse(person$militaryrank=="kapteeni","Company officer",
  ifelse(person$militaryrank=="kapteeniluutnantti","Company officer",
  ifelse(person$militaryrank=="majuri","Field officer",
  ifelse(person$militaryrank=="komentajakapteeni","Field officer",
  ifelse(person$militaryrank=="everstiluutnantti","Field officer",
  ifelse(person$militaryrank=="komentaja","Field officer",
  ifelse(person$militaryrank=="eversti","Field officer",
  ifelse(person$militaryrank=="kommodori","Field officer",
  ifelse(person$militaryrank=="kenraalimajuri","Field officer",
  ifelse(person$militaryrank=="kenraaliluutnantti","Field officer",
  ifelse(person$militaryrank=="kenraali","Field officer", NA)))))))))))))))))))))))))))
  
  return(person)
}

add_number_of_brothers_and_sisters_and_raw_katiha_data <- function(person, katihaperson) {
  # replace missing sex from our data with sex_katiha
   kp <- katihaperson %>% 
     group_by (familyId, sex) %>%
     summarise (siblings = n()) %>% na.omit()
   # make sisters and bothers tables
   sisters <- kp %>% filter (sex=="f") %>%
   dplyr::rename (sisters= siblings)
   brothers <- kp %>% filter (sex=="m") %>%
   dplyr::rename (brothers = siblings)
   # join sisters and brothers back to katiha person table
   kp <- kp %>% left_join (sisters, by = c("familyId"="familyId"))
   kp <- kp %>% left_join (brothers, by = c("familyId"="familyId")) 
   # drop unnecessary columns
   kp <- drop_columns_from_table(kp,c("sex", "sex.x", "siblings", "sex.y")) 
   # replace all NA's with 0
   kp[is.na(kp)] <- 0
   # remove duplicates
   kp <- unique(kp)
   # joinnumber of brothers and sisters back to katihapersontable
   katihaperson <- katihaperson %>% left_join(kp, by = c("familyId"="familyId")) %>%
     dplyr::rename (sex_katiha = sex)
   katihaperson <- drop_columns_from_table(katihaperson,c("birthDay", "birthMonth", "birthYear")) 
  #link person table "katihaId" to katihaperson table "id"
   person <- person %>% left_join (katihaperson, by = c("katihaId"="id"))
   person$sex <- ifelse(!is.na(person$sex_katiha), person$sex_katiha, person$sex) 
   # subtract sex of id for true brothers or sisters
   person$brothers <- ifelse(person$sex=="m",person$brothers-1,person$brothers)
   person$sisters <- ifelse(person$sex=="f",person$sisters-1,person$sisters)
   
   #add brothers and sisters to get siblings
   person$siblings <- person$brothers + person$sisters
 
   
   return(person)
}

add_birth_order <- function (person, katihaperson) {
  kp <- katihaperson %>% arrange(familyId, birthYear, birthMonth) %>%
    group_by(familyId) %>% mutate(birthorder = row_number())
  kp <- kp %>% select (id,birthorder)
  kp$birthorder <- ifelse (is.na(kp$familyId),NA,kp$birthorder)
  kp <- kp %>% drop_columns_from_table(c("familyId"))
  person <- person %>% left_join (kp, by = c("katihaId"="id"))
 
  return(person)
}

# add later the sibling details by categories you are interested in looking at
add_language_departure_type_and_birth_in_marriage <- function (person, language, departure_type, birth_in_marriage){
  person <- person %>% left_join(language, by = c("motherLanguageId"="id"))
  person <- person %>% left_join(departure_type, by = c("departureTypeId"="id")) %>%
    dplyr::rename (departureType= type)
  person <- person %>% left_join(birth_in_marriage, by =c("birthInMarriage"="code"))
  drops <- c("motherLanguageId","departureTypeId","birthInMarriage")
  person <- drop_columns_from_table(person, drops)
  return(person)
}

add_husbands_war_records <- function(person){
  husband_service <- person %>% filter (sex=='m') %>%
    select (id,injuredInWar,servedDuringWar,militaryrank_group,militaryrank_hierarchy)
  
  husband_service <- dplyr::rename(husband_service, injuredinwar_husband=injuredInWar, servedduringwar_husband=servedDuringWar,
                       militaryrank_group_husband=militaryrank_group,
                       militaryrank_hierarchy_husband=militaryrank_hierarchy)
  
  person <- left_join (person, husband_service, by=c("spouse_id"="id"))
  return(person)
}

preprocess_place_table <- function(place, pops) {
  place <- fix_encoding_in_place_table(place)
  
  # edit region name in place table
  # make region variable lower case
  place$region <- tolower(place$region)
  place$region <- gsub("viekijC#B$rvi", NA, place$region)
  place$region <- gsub("karjala", "karelia", place$region)
  place$region <- gsub("russia", "other", place$region)
  
  #subset 
  place <- place %>% select(c("id", "name", "region",
                              "extractedName", "latitude",
                              "longitude", "ambiguousRegion")) %>% dplyr::rename(place_id=id)
  # add population table and profession details, use region from populations
  # count the unique values of places in places
  place <- place %>% left_join(pops, by= c("name"="name"))
  # next just combine the region.x and region.y columns into a single region column while
  # dumping the NA's
  place$region <- place$region.x
  place$region[is.na(place$region)] <- place$region.y[is.na(place$region)]
  #select the columns we want
  place <- select(place,c("place_id", "name", "ambiguousRegion", "frequency", "population", "latitude.y", "longitude.y", "region.y")) 
  place$lat <- place$latitude.y
  place$latitude.y <- NULL
  place$lon <- place$longitude.y
  place$longitude.y <- NULL
  
  drops <- c("region.x, region")
  place <- drop_columns_from_table(place, drops)
  
  place <- dplyr::rename(place, region = region.y)
  
  return(place)
}

preprocess_profession_table <- function(profession, jobs) {
  profession <- fix_encoding_in_profession_table(profession)
  profession <- select(profession, 1:2) %>% dplyr::rename(profession_id=id)
  
  profession <- profession %>% left_join(jobs,by=c("name"="profession"))
  profession <- profession %>% select(1,2,4,5,6,7,8,9,10,3) %>% arrange(desc(frequency))
  
  return(profession)
}

preprocess_livingrecord_table <- function(livingrecord) {
  livingrecord <- select(livingrecord,2:5)
  
  return(livingrecord)
}

preprocess_person_table <- function(person) {
  person <- select(person, "id", "sex", "primaryPerson", "birthYear", "birthPlaceId",
                   "ownHouse", "professionId", "returnedKarelia", "kairaId",
                   "farmDetailsId", "lotta", "servedDuringWar", "injuredInWar","previousMarriages",
                   "foodLotta","officeLotta","nurseLotta","antiairLotta","pikkulotta","organizationLotta",
                   "martta","katihaId","militaryRankId")
  
  return(person)
}

preprocess_marriage_table <- function(marriage) {
  drops <- c("id", "markRowForRemoval", "editLog")
  marriage <- drop_columns_from_table(marriage, drops)
  
  return(marriage)
}

preprocess_child_table <- function(child) {
  child <- select(child, "id", "sex", "birthYear", "birthPlaceId", "primaryParentId", "spouseParentId")
  child <- dplyr::rename(child, child_id = id)
  
  return(child)
}

postprocess_person_table <- function(person) {
  person$sex <- gsub("m",1,person$sex)
  person$sex <- gsub("f",0,person$sex)
  person$sex <- as.integer(person$sex)
  
  names(person) <- tolower(names(person))
  
  drops <- c("kairaid", "firstname", "lastname", "formersurname")
  person <- drop_columns_from_table(person, drops)
  
  return(person)
}

read_names_from_disk <- function(path="") {
  path <- ""
  workdir <- getwd()
  setwd("~")
  karelian_names <- read.csv(paste(path, "siirtokarjalaiset_I_persons.csv", sep=""), encoding="UTF-8")
  karelian_names <- rbind(karelian_names, read.csv(paste(path, "siirtokarjalaiset_II_persons.csv", sep=""), encoding="UTF-8"))
  karelian_names <- rbind(karelian_names, read.csv(paste(path, "siirtokarjalaiset_III_persons.csv", sep=""), encoding="UTF-8"))
  karelian_names <- rbind(karelian_names, read.csv(paste(path, "siirtokarjalaiset_IV_persons.csv", sep=""), encoding="UTF-8"))
  setwd(workdir)
  
  karelian_names <- dplyr::rename(karelian_names, firstName = firstNames, lastName = lastNames)
  return(karelian_names)
}

drop_columns_from_table <- function(table, names_to_drop) {
  return(table[, !(names(table) %in% names_to_drop)])
}

drop_every_column_except_for <- function(table, names_to_keep) {
  return(table[, (names(table) %in% names_to_keep)])
}

get_data_from_server_and_preprocess_it <- function(time_download=FALSE) {
  print("Loading dependencies.")
  initialize_libraries()
  print("Establishing database connection.")
  connection <- establish_database_connection()
  
  start_time <- Sys.time()
  
  # Downloading tables form the server
  print("Downloading tables from DB.")
  person <- dbReadTable(connection, "Person")
  print("Person table downloaded.")
  profession <- dbReadTable(connection, "Profession")
  print("Profession table downloaded.")
  marriage <- dbReadTable(connection, "Marriage")
  print("Marriage table downloaded.")
  child <- dbReadTable(connection, "Child")
  print("Child table downloaded.")
  livingrecord <- dbReadTable(connection, "LivingRecord")
  print("LivingRecord table downloaded.")
  place<- dbReadTable(connection, "Place")
  print("Place table downloaded.")
  farms <- dbReadTable(connection, "FarmDetails")
  print("Farms table downloaded.")
  militaryranks <- dbReadTable(connection, "MilitaryRank")
  print("Military ranks table downloaded")
  katihaperson <- dbGetQuery(connection, "SELECT * FROM \"katiha\".\"KatihaPerson\";")
  print("Katiha person table downloaded")
  departuretype <- dbGetQuery(connection, "SELECT * FROM \"katiha\".\"DepartureType\";")
  print("DepartureType table downloaded")
  language <- dbGetQuery(connection, "SELECT * FROM \"katiha\".\"Language\";")
  print("Language table downloaded")
  birthinmarriagecode <- dbGetQuery(connection, "SELECT * FROM \"katiha\".\"BirthInMarriageCode\";")
  print("Birth in marriage code table downloaded")
  family <- dbGetQuery(connection, "SELECT * FROM \"katiha\".\"Family\";")
  print("Family table downloaded")
  end_time <- Sys.time()
  
  if (time_download) {
    print(paste("Download took:", as.numeric(end_time - start_time, digits=3)))
  }
  
  #make column names lower case
  #names(place)<- tolower(names(place))
  
  print("Downloading pops from Googlesheets.")
  pops <- load_pops_from_googlesheets()
  print("Downloading jobs from Googlesheets.")
  jobs <- load_jobs_from_googlesheets()
  
  print("Preprocessing Place table.")
  place <- preprocess_place_table(place, pops)
  print("Preprocessing Profession table.")
  profession <- preprocess_profession_table(profession, jobs)
  
  print("Preprocessing Marriage table.")
  marriage <- preprocess_marriage_table(marriage)
  print("Preprocessing Child table.")
  child <- preprocess_child_table(child)
  
  #print("Loading names from disk.")
  #karelian_names <- read_names_from_disk()
  print("Preprocessing Person table.")
  person <- preprocess_person_table(person)
  #print("Merging names to Person table.")
  #person <- merge(person, karelian_names, by="kairaId")
  print("Adding spouse IDs to Person table.")
  person <- add_spouse_ids_to_person_table(person, marriage)
  print("Adding number of children to Person table.")
  person <- add_number_of_children_to_person_table(person, child)
  print("Adding birthplace data to Person table.")
  person <- add_birthplace_data_to_person_table(person, place)
  print("Adding professions to Person table.")
  person <- add_professions_to_person_table(person, profession)
  print("Adding LivingRecord data to Person table.")
  person <- add_livingrecord_data_to_person_table(person, livingrecord)
  print("Adding outbred to Person table.")
  person <- add_outbred_to_person_table(person)
  print("Adding sons and daughters to Person table.")
  person <- add_sons_and_daughters_to_person_table(person, child)
  print("Adding age in 1970 and at marriage to Person table.")
  person <- add_age_in_1970_and_at_marriage(person)
  print("Adding couple ID to Person table.")
  person <- add_couple_id_to_person_table(person)
  print("Adding farm areas to Person table.")
  person <- add_hectares_to_person_table(person, farms)
  print("Adding returned to karelia for spouses to Person table.")
  person <- add_returned_to_karelia_for_spouses(person)
  print("Adding FDF and RDK to Person table.")
  person <- add_first_and_last_destinations_to_person_table(person, pops, livingrecord, place)
  print("Adding total peacetime migrations to Person table.")
  person <- add_total_peacetime_migrations(person)
  print("Adding previous marriages flag")
  person <- add_previous_marriages_flag(person)
  print("Adding first child YOB to Person table.")
  person <- add_first_child_birth_year(person, child)
  print("Adding last child YOB to person table.")
  person <- add_last_child_birth_year(person, child)
  print("Adding age at first birth to Person table.")
  person <- add_age_at_first_birth(person)
  print("Adding military ranks to persons table")
  person <- add_military_ranks_to_persons_table(person, militaryranks)
  print("Adding number of brothers and sisters")
  person <- add_number_of_brothers_and_sisters_and_raw_katiha_data(person, katihaperson)
  print("Adding birth order")
  person <- add_birth_order(person, katihaperson)
  print("adding some useless variables")
  person <- add_language_departure_type_and_birth_in_marriage(person, language, departuretype, birthinmarriagecode)
  print ("adding husband war records")
  person <- add_husbands_war_records(person)
  print("Postprocessing Person table.")
  person_postprocessed <- postprocess_person_table(person)
  return(person_postprocessed)
}


person_data <- get_data_from_server_and_preprocess_it()


saveRDS(person_data, "~/person_data.rds")

#saveRDS(person_data, "~/person_data.rds")