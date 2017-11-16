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
  pops <- select(pops,1:4,6:8)
  return(pops)
}

load_jobs_from_googlesheets <- function() {
  # Read in the csv files populations and occupations with social status

  # get the professions google sheet
  jobs <- gs_title("Occupations")

  # get the sheet
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
    left_join(marriage, by=c("id"="manId")) %>% 
    #rename the womanId variable to wife_id
    rename(wife_id=womanId) %>%
    # add the married women
    left_join(marriage, by=c("id"="womanId")) %>% 
    #rename the womanId variable to wife_id
    rename(husband_id=manId) 
  
  # next just combine the wife and husband columns into a spouse id column while
  # dumping the NA's
  person$spouse_id <- person$wife_id
  person$spouse_id[is.na(person$spouse_id)] <- person$husband_id[is.na(person$spouse_id)]
  # repeat for the wedding year
  person$weddingyear <- person$weddingYear.x
  person$weddingyear[is.na(person$weddingyear)] <- person$weddingYear.y[is.na(person$weddingyear)]
  
  drops <- c("wife_id", "husband_id", "weddingYear.x", "weddingYear.y")
  person <- drop_columns_from_table(person, drops)
  
  return(person)
}

add_number_of_children_to_person_table <- function(person, child) {
  # add number of kids for dads
  child2 <- child %>%
    group_by(fatherId) %>%
    summarise(dads_kids=n()) 
  # add number of kids for moms
  child3 <- child %>%
    group_by(motherId) %>%
    summarise(moms_kids=n())
  
  # row_bind the two tables
  num_kids <- bind_rows(child2,child3)
  #combine the columns kids
  num_kids$kids <- num_kids$dads_kids
  num_kids$kids[is.na(num_kids$kids)] <- num_kids$moms_kids[is.na(num_kids$kids)]
  #combine the columns mother and father ids
  num_kids$id <- num_kids$fatherId
  num_kids$id[is.na(num_kids$id)] <- num_kids$motherId[is.na(num_kids$id)]
  #delete redundant columns
  num_kids <- num_kids %>% select (6,5) %>%
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
                                        "weddingyear", "kids", "birthplace", "birthpopulation",
                                        "birthregion", "birthlat", "birthlon", "spouse_id")
  person <- person %>%
    left_join(person_trimmed, by=c("id"="spouse_id"))
  
  person <- dplyr::rename(person,
                          birthYear = birthYear.x, professionId = professionId.x,
                          returnedKarelia = returnedKarelia.x, weddingyear = weddingyear.x,
                          kids = kids.x, birthplace = birthplace.x, birthpopulation = birthpopulation.x,
                          birthregion = birthregion.x, birthlat = birthlat.x, birthlon = birthlon.x,
                          birthYear_spouse = birthYear.y, professionId_spouse = professionId.y,
                          returnedKarelia_spouse = returnedKarelia.y, weddingyear_spouse = weddingyear.y,
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
  person_filt <- person %>% select("social_class", "education", "spouse_id")
  person <- person_filt %>%
    left_join(person, by=c("spouse_id"="id"))
  
  person <- dplyr::rename(person, education = education.x, social_class = social_class.x,
                          education_spouse = education.y, social_class_spouse = social_class.y,
                          id = spouse_id.y)
  
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

add_sons_and_daughters_to_person_table <- function(person, child) {
  # add sons and daughters to m table
  # add number of kids for dads
  father <- child %>%
    group_by(fatherId, sex) %>%
    summarise(dads_kids=n()) 
  # add number of kids for moms
  mother <- child %>%
    group_by(motherId, sex) %>%
    summarise(moms_kids=n())
  
  father$daughters <- ifelse(father$sex == "f", father$dads_kids, 0)
  father$sons <- ifelse(father$sex == "m", father$dads_kids, 0)
  fathersSons <- aggregate(father$sons, by=list(father$fatherId), FUN=sum)
  fathersDaughters <- aggregate(father$daughters, by=list(father$fatherId), FUN=sum)
  fathersSons <- dplyr::rename(fathersSons, fatherId = Group.1, sons = x)
  fathersDaughters <- dplyr::rename(fathersDaughters, fatherId = Group.1, daughters = x)
  fathersKids <- merge(fathersSons, fathersDaughters, by="fatherId")
  
  mother$daughters <- ifelse(mother$sex == "f", mother$moms_kids, 0)
  mother$sons <- ifelse(mother$sex == "m", mother$moms_kids, 0)
  mothersSons <- aggregate(mother$sons, by=list(mother$motherId), FUN=sum)
  mothersDaughters <- aggregate(mother$daughters, by=list(mother$motherId), FUN=sum)
  mothersSons <- dplyr::rename(mothersSons, motherId = Group.1, sons = x)
  mothersDaughters <- dplyr::rename(mothersDaughters, motherId = Group.1, daughters = x)
  mothersKids <- merge(mothersSons, mothersDaughters, by="motherId")

  fathersKids <- dplyr::rename(fathersKids, id = fatherId)
  person <- left_join(person, fathersKids, by=c("id"="id"))
  mothersKids <- dplyr::rename(mothersKids, id = motherId)
  person <- left_join(person, mothersKids, by=c("id"="id"))
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

preprocess_place_table <- function(place, pops) {
  place <- fix_encoding_in_place_table(place)
  
  # edit region name in place table
  # make region variable lower case
  place$region <- tolower(place$region)
  place$region <- gsub("viekijC#B$rvi", NA, place$region)
  place$region <- gsub("karjala", "karelia", place$region)
  place$region <- gsub("russia", "other", place$region)
  
  #subset 
  place <- place %>% select(1:3,5,7:9) %>% rename(place_id=id)
  # add population table and profession details, use region from populations
  # count the unique values of places in places
  place <- place %>% left_join(pops, by= c("name"="name"))
  # next just combine the region.x and region.y columns into a single region column while
  # dumping the NA's
  place$region <- place$region.x
  place$region[is.na(place$region)] <- place$region.y[is.na(place$region)]
  #select the columns we want
  place <- select(place,1:2,7,9,11:14) 
  place$lat <- place$latitude.y
  place$latitude.y <- NULL
  place$lon <- place$longitude.y
  place$longitude.y <- NULL
  
  return(place)
}

preprocess_profession_table <- function(profession, jobs) {
  profession <- fix_encoding_in_profession_table(profession)
  profession <- select(profession, 1:2) %>% rename(profession_id=id)
  
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
                           "farmDetailsId", "lotta", "servedDuringWar", "injuredInWar")
  
  return(person)
}

preprocess_marriage_table <- function(marriage) {
  marriage <- select(marriage, 2:4)
  
  return(marriage)
}

preprocess_child_table <- function(child) {
  child <- select(child, 1,4,5,6,7,8)
  child <- rename(child, child_id = id)
  
  return(child)
}

postprocess_person_table <- function(person) {
  person$returnedKarelia <- gsub("true",1, person$returnedKarelia)
  person$returnedKarelia <- gsub( "false",0, person$returnedKarelia)
  person$returnedKarelia <- gsub( "unknown",NA, person$returnedKarelia)
  person$returnedKarelia <- as.integer(person$returnedKarelia)
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
  initialize_libraries()
  connection <- establish_database_connection()
  
  start_time <- Sys.time()
  
  person <- dbReadTable(connection, "Person")
  profession <- dbReadTable(connection, "Profession")
  marriage <- dbReadTable(connection, "Marriage")
  child <- dbReadTable(connection, "Child")
  livingrecord <- dbReadTable(connection, "LivingRecord")
  place<- dbReadTable(connection, "Place")
  farms <- dbReadTable(connection, "FarmDetails")

  end_time <- Sys.time()
  
  if (time_download) {
    print(paste("Download took:", as.numeric(end_time - start_time, digits=3)))
  }
  
  #make column names lower case
  #names(place)<- tolower(names(place))
  
  pops <- load_pops_from_googlesheets()
  jobs <- load_jobs_from_googlesheets()
  
  place <- preprocess_place_table(place, pops)
  profession <- preprocess_profession_table(profession, jobs)
  
  marriage <- preprocess_marriage_table(marriage)
  child <- preprocess_child_table(child)
  
  karelian_names <- read_names_from_disk()
  person <- preprocess_person_table(person)
  person <- merge(person, karelian_names, by="kairaId")
  person <- add_spouse_ids_to_person_table(person, marriage)
  person <- add_number_of_children_to_person_table(person, child)
  person <- add_birthplace_data_to_person_table(person, place)
  person <- add_professions_to_person_table(person, profession)
  person <- add_livingrecord_data_to_person_table(person, livingrecord)
  person <- add_outbred_to_person_table(person)
  person <- add_sons_and_daughters_to_person_table(person, child)
  person <- add_age_in_1970_and_at_marriage(person)
  person <- add_couple_id_to_person_table(person)
  person <- add_hectares_to_person_table(person, farms)
  person_postprocessed <- postprocess_person_table(person)
  return(person_postprocessed)
}

person_data <- get_data_from_server_and_preprocess_it()
saveRDS(person_data, "~/person_data.rds")