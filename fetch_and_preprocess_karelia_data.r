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
    left_join(marriage, by=c("id"="manId")) %>% 
    #rename the womanId variable to wife_id
    dplyr::rename(wife_id=womanId) %>%
    # add the married women
    left_join(marriage, by=c("id"="womanId")) %>% 
    #dplyr::rename the womanId variable to wife_id
    dplyr::rename(husband_id=manId) 
  
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
    child_m <- child %>%
      group_by (motherId) %>%
      filter(birthYear == min(birthYear)) %>%
      group_by (motherId) %>%
      filter(child_id==min(child_id))
    person2<- person %>%
      left_join (child_m , by= c("id"="motherId")) %>%
      select ("id","birthYear.y") %>%
     dplyr::rename (mothers_first_child_YOB= birthYear.y)
    
  # add children for father ids
    child_f <- child %>%
      group_by (fatherId) %>%
      filter (birthYear == min(birthYear)) %>%
      group_by (fatherId) %>%
        filter (child_id==min(child_id))
    person3<- person2 %>%
      left_join (child_f , by =c("id"="fatherId")) %>%
      select ("id","mothers_first_child_YOB","birthYear") %>%
    dplyr::rename ( fathers_first_child_YOB= birthYear)
    
    
    person3$first_child_YOB <- ifelse(is.na(person3$mothers_first_child_YOB), 
                                      person3$fathers_first_child_YOB, person3$mothers_first_child_YOB)
    
    person4 <- person3 %>% select ("id","first_child_YOB")
    
    person<- person %>% left_join (person4, by=c("id"="id"))
    return(person)
}

add_age_at_first_birth <- function(person) {
  person$age_at_first_birth<- person$first_child_YOB - person$birthYear

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
                   "farmDetailsId", "lotta", "servedDuringWar", "injuredInWar")
  
  return(person)
}

preprocess_marriage_table <- function(marriage) {
  marriage <- select(marriage, 2:4)
  
  return(marriage)
}

preprocess_child_table <- function(child) {
  child <- select(child, 1,4,5,6,7,8)
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
  
  
  print("Adding first child YOB to Person table.")
  person <- add_first_child_birth_year(person, child)
  
  print("Adding age at first birth to Person table.")
  person <- add_age_at_first_birth(person)
  
  print("Postprocessing Person table.")
  person_postprocessed <- postprocess_person_table(person)
  return(person_postprocessed)
}


person_data <- get_data_from_server_and_preprocess_it()


saveRDS(person_data, "~/person_data.rds")

#saveRDS(person_data, "~/person_data.rds")