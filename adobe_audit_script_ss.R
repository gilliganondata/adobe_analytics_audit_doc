# Capturing the start time -- the final console output will be how many minutes
# it took for the process to run.
monitorStartTime <- Sys.time()

# Check for packages needed and then load the packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(RSiteCatalyst, tidyverse, sqldf, data.table, tcltk, lubridate, WriteXLS)

# Validate that underlying Perl modules for WriteXLS are installed correctly
# Will return "Perl found. All required Perl modules were found" if installed correctly
testPerl()

client_id <- Sys.getenv("ADOBE_API_USERNAME_MAR")
client_secret <- Sys.getenv("ADOBE_API_SECRET_MAR")

SCAuth(client_id, client_secret)
RSID <- Sys.getenv("ADOBE_RSID_MAR")

end_date <- today() - days(1)
start_date <- today() - days(60)

# Cutoff for total instances below which will flag as "Minimal Data"
min_instances <- 100

##############################################################################################
## Props - SDR, Factors, and Usage
##############################################################################################

SC_Props <- GetProps(RSID)

# # Temporarily just get the first 5
# SC_Props <- SC_Props[1:5,]

# Add few new columns to SC_Props to hold uniqueVals and instance counts. Set all rows to 0 initially
SC_Props$uniqueVals <- 0
SC_Props$instances <- 0

# Change this data frame to use the prop id as rownames
row.names(SC_Props)<-SC_Props$id

ids <- SC_Props[grep("TRUE", SC_Props$enabled, ignore.case=T),][["id"]]
print(ids)

# Loop through and pull and process the data
for (prop in ids) { 
  rptName <-paste("dtl_", prop, sep="")
  print(rptName)
  
  rankedRpt <- QueueRanked(reportsuite.id = RSID,
                           date.from = start_date,
                           date.to = end_date,
                           metrics = c("instances"),
                           elements = c(prop),
                           top =c(5000)
  )
  SC_Props[prop,"uniqueVals"] <-nrow(rankedRpt)
  SC_Props[prop,"instances"] <-sum(rankedRpt$instances)
  if ( dim(rankedRpt)[1]==0 ) {
    SC_Props[prop,"example_1"] <- "NA"
    SC_Props[prop,"example_2"] <- "NA"
    SC_Props[prop,"example_3"] <- "NA"
    SC_Props[prop,"example_4"] <- "NA"
    SC_Props[prop,"example_5"] <- "NA"
  } else {
    SC_Props[prop,"example_1"] <-paste0(rankedRpt[1,1], " (", format(rankedRpt[1,3], big.mark=","), ")")
    SC_Props[prop,"example_2"] <-paste0(rankedRpt[2,1], " (", format(rankedRpt[2,3], big.mark=","), ")")
    SC_Props[prop,"example_3"] <-paste0(rankedRpt[3,1], " (", format(rankedRpt[3,3], big.mark=","), ")")
    SC_Props[prop,"example_4"] <-paste0(rankedRpt[4,1], " (", format(rankedRpt[4,3], big.mark=","), ")")
    SC_Props[prop,"example_5"] <-paste0(rankedRpt[5,1], " (", format(rankedRpt[5,3], big.mark=","), ")")
  }
}

# Add Data Flag
SC_Props <- SC_Props %>% 
  mutate(data_status = ifelse(uniqueVals == 0, "No Data",
                                ifelse(instances < min_instances, "Minimal Data",
                                       "Has Data")))

# Get subset of results
SC_Props_Final <- SC_Props %>% 
  select(id, name, description, pathing_enabled, list_enabled, participation_enabled,
         data_status, uniqueVals, instances, example_1, example_2, example_3,
         example_4, example_5) %>% 
  # Replace line and carriage returns with nothing
  mutate(description = gsub("\\r\\n","", description))


write.csv(SC_Props_Final, paste("SC_Props_wwrs_",RSID,"_",today(),".csv",sep=""))

##############################################################################################
## eVars -  SDR, Factors, and Usage
##############################################################################################
SC_Evars<-GetEvars(RSID)

# # Temporarily just get the first 5
# SC_Evars <- SC_Evars[1:3,]

#Add few new columns to SC_Evars to hold uniqueVals and instance counts. Set all rows to 0 initially
SC_Evars$uniqueVals <-0
SC_Evars$instances <- 0
if ('merchandising_syntax' %in% colnames(SC_Evars) == FALSE) {
  SC_Evars$merchandising_syntax <- ""
}
#Change this data frame to use the evar id as rownames
row.names(SC_Evars)<-SC_Evars$id

ids <- SC_Evars[grep("TRUE", SC_Evars$enabled, ignore.case=T),][["id"]]
print(ids)

for (evar in ids ) { 
  rptName <-paste("dtl_", evar, sep="")
  print(rptName)
  
  rankedRpt <- QueueRanked(reportsuite.id = RSID,
                           date.from = start_date,
                           date.to = end_date,
                           metrics = c("instances"),
                           elements = c(evar),
                           top =c(5000)
  )
  SC_Evars[evar,"uniqueVals"] <-nrow(rankedRpt)
  SC_Evars[evar,"instances"] <-sum(rankedRpt$instances)
  
  if ( dim(rankedRpt)[1]==0 ) {
    SC_Evars[evar,"example_1"] <- "NA"
    SC_Evars[evar,"example_2"] <- "NA"
    SC_Evars[evar,"example_3"] <- "NA"
    SC_Evars[evar,"example_4"] <- "NA"
    SC_Evars[evar,"example_5"] <- "NA"
  } else {
    SC_Evars[evar,"example_1"] <- paste0(rankedRpt[1,1], " (", format(rankedRpt[1,3], big.mark = ","), ")")
    SC_Evars[evar,"example_2"] <- paste0(rankedRpt[2,1], " (", format(rankedRpt[2,3], big.mark = ","), ")")
    SC_Evars[evar,"example_3"] <- paste0(rankedRpt[3,1], " (", format(rankedRpt[2,3], big.mark = ","), ")")
    SC_Evars[evar,"example_4"] <- paste0(rankedRpt[4,1], " (", format(rankedRpt[2,3], big.mark = ","), ")")
    SC_Evars[evar,"example_5"] <- paste0(rankedRpt[5,1], " (", format(rankedRpt[2,3], big.mark = ","), ")")
  }
  
}

# Add Data Flag
SC_Evars <- SC_Evars %>% 
  mutate(data_status = ifelse(uniqueVals == 0, "No Data",
                              ifelse(instances < min_instances, "Minimal Data",
                                     "Has Data")))

# Get subset of results
SC_Evars_Final <- SC_Evars %>% 
  select(id, name, description, type, expiration_type, expiration_custom_days,
         allocation_type, merchandising_syntax, data_status, uniqueVals, instances, 
         example_1, example_2, example_3, example_4, example_5) %>% 
  # Replace line and carriage returns with nothing
  mutate(description = gsub("\\r\\n","", description))


write.csv(SC_Evars_Final, paste("SC_Evars_wwrs_",RSID,"_",today(),".csv",sep=""))


##############################################################################################
## events -  SDR, Factors, and Usage
##############################################################################################
SC_Events<-GetSuccessEvents(RSID)
SC_Events$evtTotal <- 0
#Change this data frame to use the event id as rownames
row.names(SC_Events)<-SC_Events$id

#omit all disabled events
ids<-SC_Events[grep("disabled", SC_Events$type, ignore.case=T, invert=TRUE),][["id"]]
#omit the "instances" built in event as it cannot be used in overTimeRpt
ids<-ids[grep("instances",  ids, ignore.case=T, invert=TRUE)]

for (evt in ids) { 
  rptName <-paste("Pulling details for : ", evt, sep="")
  print(rptName)
  
  overTimeRpt <- QueueOvertime(reportsuite.id = RSID,
                               date.from = start_date,
                               date.to = end_date,
                               metrics = c(evt),
                               date.granularity = "year"
  )
  SC_Events[evt,"evtTotal"] <-sum(overTimeRpt[evt])
}


# Add Data Flag
SC_Events <- SC_Events %>% 
  mutate(data_status = ifelse(evtTotal == 0, "No Data",
                              ifelse(evtTotal < min_instances, "Minimal Data",
                                     "Has Data")))

# Get subset of results
SC_Events_Final <- SC_Events %>% 
  filter(type != "disabled") %>% 
  select(id, name, description, type, participation, serialization, visibility,
         polarity, data_status, evtTotal) %>% 
  # Replace line and carriage returns with nothing
  mutate(description = gsub("\\r\\n","", description))

write.csv(SC_Events_Final, paste("SC_Events_wwrs_",RSID,"_",today(),".csv",sep=""))


######################## 
# Generate a single Excel file
########################

# Create list of report suite objects, written as strings
objlist <- c("SC_Events_Final","SC_Evars_Final","SC_Props_Final")

# And...we actually want to make the worksheet names a bit cleaner
sheetNames <- c("Events","eVars","Props")

filename <- paste(RSID, " - ", as.character(start_date), " - ", as.character(end_date),  ".xlsx",sep="")

# Write out Excel file with auto-width columns, a bolded header row and filters turned on
WriteXLS(objlist, filename, SheetNames = sheetNames,
         AdjWidth = TRUE, BoldHeaderRow = TRUE, AutoFilter = TRUE)

cat(RSID,"- Output file created.\n",sep=" ")


monitorEndTime <- Sys.time()

# Write out to the console how long it took for the entire process to run.
cat("This process took",monitorEndTime - monitorStartTime,".",sep=" ")

