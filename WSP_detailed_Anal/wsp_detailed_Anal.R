# Read and transfer one file at a time

# library(tidyverse);library(readr);library(RDCOMClient) ;library(data.table)
library(DescTools)

options(scipen = 100) # Ensure non-scientific display of numbers 
getFilesFromDir    = 'C:/SPORTSBET daily wagering files/ProcessedAll' #get SportsBet processed files from this dir
moveCleansedToDir  =  'x: '                                                #once files have been processed and writtent to warehouse test dbase, move originating file to 

# get master list of processed files and sort by alphabetical order
d = dir(path = getFilesFromDir , pattern = '.CSV',ignore.case = TRUE,all.files = FALSE,include.dirs = FALSE) 
d = sort(d)

# name of first file 
d[1]

# Try to load contents of first file and catch errors 
fileCanBeRead = function (dirPath, nameOfFileToRead) { 
  file_to_read = paste0(dirPath,'/' ,nameOfFileToRead)
  rd = try(data.table::fread(file_to_read)) 
  
  # Catch file read errors 
  if ( 'try-error' %in% class(rd)) {
    message('Err: Caught an error during file read\n')
    stop() }
  else {
    message('OK: file file successfully read')
    return(data.table::fread(file_to_read))
  }
}

rd = fileCanBeRead(getFilesFromDir, d[1])

# Check that WSP is using consistent names 
fNameTest = function (rreadObj) {
  fileNamesTest= 0
  
  fileNamesTest[1]<-  names(rreadObj)[1] == 'Name of WSP'
  fileNamesTest[2]<-  names(rreadObj)[2] == 'Event Date' 
  fileNamesTest[3]<-  names(rreadObj)[3] == 'Bet Result Date'    
  fileNamesTest[4]<-  names(rreadObj)[4] == 'Venue'
  fileNamesTest[5]<-  names(rreadObj)[5] == 'Race Number' 
  fileNamesTest[6]<-  names(rreadObj)[6] == 'Parimutuel Bets Taken' 
  fileNamesTest[7]<-  names(rreadObj)[7] == 'Non-Parimutuel Bets Taken' 
  fileNamesTest[9]<-  names(rreadObj)[9] == 'Net Customer Winnings' 
  fileNamesTest[8]<-  names(rreadObj)[8] == 'Bets Paid/credited to customers' 
  fileNamesTest[10]<- names(rreadObj)[10] == 'Bets Back' 
  fileNamesTest[11]<- names(rreadObj)[11] == 'Bet Back Revenue' 
  fileNamesTest[12]<- names(rreadObj)[12] == 'Other Revenue' 
  fileNamesTest[13]<- names(rreadObj)[13] == 'Jackpots Created'

  
  if (sum(fileNamesTest)== length(fileNamesTest)){
    message('OK: file name columns are consistent')
  } else {
    message('Err: file name columns have changed')
  }
}

fNameTest(rd)

# Write file Name and Date file processed by GRV to data supplied
rd$file_name =d[1]
rd$date_file_processed_grv = Sys.Date()

Expected_NameOfWSP <- function (Expected_NameOfWSP) {
  if (unique(rd$`Name of WSP`)== Expected_NameOfWSP ){
    message('OK: WSP name is consistent')
  }else {
    message('Err: WSP name has changed')
  }
}

Expected_NameOfWSP('Sportsbet')

MakeSureDatesAreCorrect =  function(dateColToTest){
  result = try(lubridate::dmy(dateColToTest))
  if (class(result) ==  'Date') {
    message('Ok: All items are dates: ' )
    result
  }else {
    message(paste('Err: All these are not dates: ' ))
    dateColToTest
  }
}

rd$`Event Date`     =  MakeSureDatesAreCorrect(rd$`Event Date`)
rd$`Bet Result Date`=  MakeSureDatesAreCorrect(rd$`Bet Result Date`)

# Make sure data to write back is only a data frame 

isObjectOnlyDataFrame = function(fileOjbect)
  if (is.data.frame(fileOjbect)){
    message('OK: data is only a data frame')
    TRUE
  }else{
    message('Err: data is only a data frame')
    FALSE}

isObjectOnlyDataFrame(rd)

# ---------------
# There must be at least 1 row's worth of data to read 

isObjectOnlyDataFrameAndAtLeastOneRowData = function(fileOjbect)
  if (is.data.frame(fileOjbect) & nrow(fileOjbect)>0 ){
    message("OK: data is only a data frame and hase at least 1 row of data")
    TRUE
  }else{
    message("Err: data is not only a data frame and doesn't have more than 1 row of data")
    FALSE}

isObjectOnlyDataFrameAndAtLeastOneRowData(rd)

# ---------------
# Rename Columns In a Consistent Way 
ConsistantColNames = function(){
  as.character(c(
     'name_of_wsp'
    ,'event_date'
    ,'bet_result_date'
    ,'venue'
    ,'race_number'
    ,'pari_bets_taken'
    ,'non_pari_bets_taken'
    ,'bets_paid_credited_to_customers'
    ,'net_customer_winnings'
    ,'bets_back'
    ,'bet_back_rev'
    ,'other_rev'
    ,'jackpots_created'
  )
  )
}  

# Rename columns to consistent names
names(rd)[1:13] = ConsistantColNames()

# expected data types 
number_of_cols = 15
CorrDataTypes = function(){
  CorrectDType=vector(mode = 'logical' , number_of_cols)  
  
  CorrectDType[1]  =  is.character(rd$name_of_wsp)
  CorrectDType[2]  =  DescTools::IsDate(rd$event_date)
  CorrectDType[3]  =  DescTools::IsDate(rd$bet_result_date)
  CorrectDType[4]  =  is.character(rd$venue)
  CorrectDType[5]  =  is.integer(rd$race_number)
  CorrectDType[6]  =  is.numeric(rd$pari_bets_taken)
  CorrectDType[7]  =  is.numeric(rd$non_pari_bets_taken)
  CorrectDType[8]  =  is.numeric(rd$bets_paid_credited_to_customers) 
  CorrectDType[9]  =  is.numeric(rd$net_customer_winnings)
  CorrectDType[10] =  is.numeric(rd$bets_back)
  CorrectDType[11] =  is.numeric(rd$bet_back_rev)
  CorrectDType[12] =  is.numeric(rd$other_rev)
  CorrectDType[13] =  is.numeric(rd$jackpots_created)
  CorrectDType[14] =  is.character(rd$file_name)
  CorrectDType[15] =  DescTools::IsDate(rd$date_file_processed_grv)
  
  if (sum(CorrectDType) == number_of_cols ) {
    message('OK: Column names and data types are correct ')}
  else{   message('Err: Column names and data types not as expected') }
}
CorrDataTypes()

# -----------------------
# coerce to correct data types 
rd$pari_bets_taken                  =  as.double(rd$pari_bets_taken)
rd$non_pari_bets_taken              =  as.double(rd$non_pari_bets_taken)
rd$bets_paid_credited_to_customers  =  as.double(rd$bets_paid_credited_to_customers) 
rd$net_customer_winnings            =  as.double(rd$net_customer_winnings)
rd$bets_back                        =  as.double(rd$bets_back)
rd$bet_back_rev                     =  as.double(rd$bet_back_rev)
rd$other_rev                        =  as.double(rd$other_rev)
rd$jackpots_created                 =  as.double(rd$jackpots_created)


# ----------------------
# Old style solution below, assuming all csv's are readable - stacking one on top of another

mf = data.frame(fNumb=seq_along(d) , fName=d, isDataFrame= FALSE)

# Which files are readable as csv's 
fil = vector(mode='list', length(d))

for (i in seq_along(d)){
  # print(d[i])
  fil[[i]] = try({readr::read_csv(d[i]) })
  fil[[i]]$fileName = d[i]
} 


library(dplyr)
xx = do.call(dplyr::bind_rows, fil[which(sapply(fil,ncol)==14)])
str(xx)
summary(xx$`Name of WSP`)


head(sort(xx$`Bet Result Date`))
tail(sort(xx$`Bet Result Date`))

xx$`Event Date` = anytime::anydate(xx$`Event Date`)
xx$`Bet Result Date` = anytime::anydate(xx$`Bet Result Date`)

summary(xx$`Event Date`)
summary(xx$`Bet Result Date`)

xx[is.na(xx$`Event Date`),]

as.data.frame(names(xx),)

xx = fread('20150701_051101_20150701_040138_Sportsbet_11042015.csv')
str(xx)
lubridate::dmy(xx$`Event Date`)



fil
for (i in fil) {
  #print(i)
  class(i)
}

table(unlist(sapply(fil,class)))


fil[[1]][,10:14]


fil[1]

ul = unlist(fil)
str(ul)

str(fil[[1]])

do.call(rbind, fil[1:300] )

# str(fil[[1]][2])
as.data.frame(table(unlist(fil)))
plyr::count(sapply(fil, '[' , 2) )

mf$isDataFrame = ifelse(sapply(fil, '[' , 2) == 'data.frame', TRUE, FALSE)

filesToFix = mf[is.na(mf$isDataFrame),]

FilesCanBeReadAsTables = d[dt]

fil2      = vector(mode='numeric', length(fil))
fil2names = vector(mode='character', length(file))

for (i in seq_along(FilesCanBeReadAsTables)){
  fil2[[i]] = try({ncol(fread(FilesCanBeReadAsTables[i]) )})
  fil2names[i] = FilesCanBeReadAsTables[i]
}
table(fil2)

fil3 = vector(mode='list')  
for (i in seq_along(FilesCanBeReadAsTables)){
  fil3[[i]] = try({fread(FilesCanBeReadAsTables[i] )})
  fil3[[i]]$FileName = FilesCanBeReadAsTables[i]
}  

sapply(fil3 ,  ncol)
table(sapply(fil3 ,  ncol))

colCnt = function(dfr) {ncol(dfr)}
table(sapply(fil3,colCnt))

correctColNames = names(fread(fil2names[1]))

for (i in seq_along(fil3)){
  print(names(fil3[[i]]))
}  
fil3  

for (i in seq_along(fil3)){
  # names(fil3[[i]]) = correctColNames
  
  # print(fil3[[i]])
  fil3[[i]]$`Event Date` = lubridate::dmy(fil3[[i]]$`Event Date`)
  fil3[[i]]$`Bet Result Date` = lubridate::dmy(fil3[[i]]$`Bet Result Date`)
  fil3[[i]]$`Race Number` = as.numeric(fil3[[i]]$`Race Number`)
  fil3[[i]]$`Parimutuel Bets Taken` = as.numeric(fil3[[i]]$`Parimutuel Bets Taken`)
  fil3[[i]]$`Non-Parimutuel Bets Taken` = as.numeric(fil3[[i]]$`Non-Parimutuel Bets Taken`)
  fil3[[i]]$`Bets Paid/credited to customers` = as.numeric(fil3[[i]]$`Bets Paid/credited to customers`)
  fil3[[i]]$`Net Customer Winnings` = as.numeric(fil3[[i]]$`Net Customer Winnings`)
  fil3[[i]]$`Bets Back` = as.numeric(fil3[[i]]$`Bets Back`)
  fil3[[i]]$`Bet Back Revenue` = as.numeric(fil3[[i]]$`Bet Back Revenue`)
  fil3[[i]]$`Other Revenue` = as.numeric(fil3[[i]]$`Other Revenue`)
  fil3[[i]]$`Jackpots Created` = as.numeric(fil3[[i]]$`Jackpots Created`)
}

ddff = do.call(rbind, fil3)
names(fil3[[244]])
names((fil3[[1]]))


#Read in one or many blocks of Excel cells, just select blocks of Excel cells run this function and they will be loaded to df
df=DescTools::XLGetRange(header = TRUE)

#Write a semi-colon delimited data block in R to Excel 
DescTools::XLView(ddff)

plyr::count(ddff$`Event Date`)


which(sapply(fil3, colCnt)!=13)
table(sapply(fil3, colCnt))
244 245 248 494 495 497 498
fil2names[494]
library(DescTools);library(RDCOMClient)

#Read in one or many blocks of Excel cells, just select blocks of Excel cells run this function and they will be loaded to df
df=DescTools::XLGetRange(header = TRUE)

#Write a semi-colon delimited data block in R to Excel
DescTools::XLView(readr::read_csv(fil2names[244]))

testFile = 248 #245 244
fil2names[testFile]
fread(fil2names[testFile])
ncol(fread(fil2names[testFile]))
ncol(fread(fil2names[testFile])[1,])

names(fread(fil2names[244]))==   names(fread(fil2names[1]))
names(fread(fil2names[244]))=   names(fread(fil2names[1]))


table(names(fil3))


df = do.call(bind_rows, fil3) 
dim(df) 
names(df)

df %>% 
  group_by(`Event Date`) %>% 
  summarise(BetsPerDay = n()) %>% 
  arrange(`Event Date`)


summary(fil3)


str(fil3[[6]])
str(fil3[[7]])



summary(df)
library(DescTools)
Desc(df)




for (i in seq_along(d)){
  # fil[[i]] = fread([[i]])
  fil[[i]] = try({fread(d[i] )})
}

do.call(try(rbind), fil[which(sapply(fil,is.data.frame))])
sapply(d ,class )


fil[which(sapply(fil,is.data.frame))]

fil[[3]]



sapply(fil, class)
summary(fil)


fread(ProcessedList[[1]])


dir()

return(ProcessedList)
}  

# ---

# Get Warehouse WSP data table and join to sftp folder names ----
# 
# require(RODBC)
# fto=odbcConnect("R_Warehouse_Prod",uid="grv_support_tlukacevic",pwd="Anna19362")
# sql = " select * from [warehouse].[d1_WageringProvider] "
# sql2=gsub(sql, pattern = '\\t|\\n',replacement = ' ' ,fixed = TRUE) #fixed = TRUE
# dfSQL2 = sqlQuery(fto,sql2 , stringsAsFactors=FALSE)  	
# dput(dfSQL2)

# -------------- warehouse table initial view
# Before forematting in notepad, gett rid of 5spaces and tabs in Notepad++
# pk.getWSP_Warehouse_Tble = 
# structure(list(d1_WageringProvider_ID = 1:26, Code = c("VICTAB", 
#                                                        "NSWTAB", "ToteTas", "ACTTAB", "RWWA", "RWWAClubs", "TattsQld", 
#                                                        "NTTAB", "SATAB", "Luxbet", "Unibet", "CrownBet", "Bet365", "Ladbrokes", 
#                                                        "Sportingbet", "Sportsbet", "Centrebet", "Palmerbet", "TomWaterhouse", 
#                                                        "Topbetta", "Topsport", "Classicbet", "Betfair", "ClubAllSports", 
#                                                        "BetHQ", "MadBookie"), Name = c("TABCORP (VIC)", "TAB Ltd (NSW)", 
#                                                                                        "TOTE Tasmania", "TABCORP (ACT)", "RWWA", "RWWA Clubs", "TattsBet", 
#                                                                                        "NT TAB Pty Ltd", "SA TAB Pty Ltd", "Luxbet", "UNI Bet (ex Betchoice)", 
#                                                                                        "Beteasy (ex Betezy)", "Bet365", "Ladbrokes (inc Bookmaker)", 
#                                                                                        "Sportingbet Aust", "Sportsbet", "Sportingbet (Centrebet)", "Palmerbet", 
#                                                                                        "Tom Waterhouse.com", "Topbetta", "Top Sport", "Classicbet", 
#                                                                                        "Betfair", "Club All Sports", "Bet HQ", "Mad Bookie"), Category = c("TAB", 
#                                                                                                                                                            "TAB", "TAB", "TAB", "TAB", "TAB", "TAB", "TAB", "TAB", "TAB", 
#                                                                                                                                                            "Corporate", "Corporate", "Corporate", "Corporate", "Corporate", 
#                                                                                                                                                            "Corporate", "Corporate", "Corporate", "Corporate", "Corporate", 
#                                                                                                                                                            "Corporate", "Corporate", "Exchange", "Corporate", "Corporate", 
#                                                                                                                                                            "Corporate"), ReportingGroup = c("VICTAB", "TAB", "TAB", "TAB", 
#                                                                                                                                                                                             "TAB", "TAB", "TAB", "TAB", "TAB", "Corp/Exch", "Corp/Exch", 
#                                                                                                                                                                                             "Corp/Exch", "Corp/Exch", "Corp/Exch", "Corp/Exch", "Corp/Exch", 
#                                                                                                                                                                                             "Corp/Exch", "Corp/Exch", "Corp/Exch", "Corp/Exch", "Corp/Exch", 
#                                                                                                                                                                                             "Corp/Exch", "Corp/Exch", "Corp/Exch", "Corp/Exch", "Corp/Exch"
#                                                                                                                                                            )), .Names = c("d1_WageringProvider_ID", "Code", "Name", "Category", 
#                                                                                                                                                                           "ReportingGroup"), row.names = c(NA, 26L), class = "data.frame")

# -------------- warehouse table cleaned up view
# After forematting in notepad, gett rid of 5spaces and tabs in Notepad++

pk.getWSP_Warehouse_Tble = 
  structure(list(
    d1_WageringProvider_ID = 1:26
    , Code = c("VICTAB", 
               "NSWTAB", "ToteTas", "ACTTAB", "RWWA", "RWWAClubs", "TattsQld", 
               "NTTAB", "SATAB", "Luxbet", "Unibet", "CrownBet", "Bet365", "Ladbrokes", 
               "Sportingbet", "Sportsbet", "Centrebet", "Palmerbet", "TomWaterhouse", 
               "Topbetta", "Topsport", "Classicbet", "Betfair", "ClubAllSports", 
               "BetHQ", "MadBookie")
    , Name = c("TABCORP (VIC)", "TAB Ltd (NSW)", 
               "TOTE Tasmania", "TABCORP (ACT)", "RWWA", "RWWA Clubs", "TattsBet", 
               "NT TAB Pty Ltd", "SA TAB Pty Ltd", "Luxbet", "UNI Bet (ex Betchoice)", 
               "Beteasy (ex Betezy)", "Bet365", "Ladbrokes (inc Bookmaker)", 
               "Sportingbet Aust", "Sportsbet", "Sportingbet (Centrebet)", "Palmerbet", 
               "Tom Waterhouse.com", "Topbetta", "Top Sport", "Classicbet", 
               "Betfair", "Club All Sports", "Bet HQ", "Mad Bookie")
    , Category = c("TAB", 
                   "TAB", "TAB", "TAB", "TAB", "TAB", "TAB", "TAB", "TAB", "Corporate", 
                   "Corporate", "Corporate", "Corporate", "Corporate", "Corporate", 
                   "Corporate", "Corporate", "Corporate", "Corporate", "Corporate", 
                   "Corporate", "Corporate", "Exchange", "Corporate", "Corporate", 
                   "Corporate")
    , ReportingGroup = c("VICTAB", "TAB", "TAB", "TAB", 
                         "TAB", "TAB", "TAB", "TAB", "TAB", "Corp/Exch", "Corp/Exch", 
                         "Corp/Exch", "Corp/Exch", "Corp/Exch", "Corp/Exch", "Corp/Exch", 
                         "Corp/Exch", "Corp/Exch", "Corp/Exch", "Corp/Exch", "Corp/Exch", 
                         "Corp/Exch", "Corp/Exch", "Corp/Exch", "Corp/Exch", "Corp/Exch"
    ))
    , .Names = c("d1_WageringProvider_ID", "Code", "Name", "Category", 
                 "ReportingGroup"), row.names = c(NA, 26L)
    , class = "data.frame")



# Listing of Top Level WSP folders ----
# Directly from fstp server: 
pk.get_WSP_dirs = function()
  c(
    'ACTTAB'
    ,'BET365'
    ,'BETEASY'
    ,'BETFAIR'
    ,'BETHQ'
    ,'CENTREBET'
    ,'CLASSICBET'
    ,'CLUBALLSPORTS'
    ,'LADBROKES'
    ,'LUXBET'
    ,'MADBOOKIE'
    ,'NSWTAB'
    ,'NTTAB'
    ,'PALMERBET'
    ,'RWWA'
    ,'RWWACLUBS'
    ,'SATAB'
    ,'SPORTSBET'
    ,'SPORTINGBET'
    ,'TATTSQLD'
    ,'TOMWATERHOUSE'
    ,'TOPBETTA'
    ,'TOPSPORT'
    ,'TOTETAS'
    ,'UNIBET'
    ,'VICTAB'
  )

# ---- End 
# Get unique Error msgs per WSP 
# eg to call pk.scan.Errfiles.in.fldr('BET365','ERROR')

pk.scan.Errfiles.in.fldr = function(wsp.Folder.Name ) {
  
  # WSPs = which(pk.get_WSP_dirs() %in% c(wsp.Folder.Name))
  sub.fldr = 'ERROR'
  err.list = list() 
  
  
  setwd(paste0("Y:/",wsp.Folder.Name,"/",sub.fldr,"/")) 
  d = dir() ; d
  
  vErrLog = grep('ERROR_LOG.CSV', d,ignore.case = TRUE) ; print(paste0('Uniq.files.in.err.fldr: ',length(vErrLog)))
  #  2  4  6  8 10 12 14 16
  try(
    for (i in 1:length(vErrLog)) {
      f = d[vErrLog[i]]  #; print(paste0(vErrLog[i], ' FName-> ',f))
      fr =readr::read_csv(f) #; fr
      # fr =data.table::fread(f) #; fr
      
      names(fr) = gsub(' ','',tolower(names(fr)),fixed = TRUE)
      names(fr) = gsub('-','',tolower(names(fr)),fixed = TRUE)
      names(fr) = gsub('/','',tolower(names(fr)),fixed = TRUE)
      namesUsed = names(fr)
      # print(f)
      # print('____________________________________________')
      # print(names(fr))
      #print(fr)
      # print(vErrLog[i])
      # print(unique(fr[c('log_severity','log_code', 'log_message')]))
      err.list[[i]] = cbind(file = as.character(f)  , err.Msg = unique(fr['log_message']))
      # print('____________________________________________')
      
    }
  )
  
  ErrList = do.call(rbind,err.list)
  ErrList$file = as.character(ErrList$file)
  return(ErrList)
}  


# ----- Pass Error log file name moves fixes error folders back to inbox 
# Assumes Error_Log file name contains stub of original data file 
# Check didn't debug much 
#  eg   pk.move.err.log.files.back.to.inbox('TOPSPORT'  , '20160515_000422_TopSport_14052016_ERROR_LOG.csv'  )

pk.move.err.log.files.back.to.inbox <- function (wsp.fldr.name, err.file) {
  
  # Move the data file back out of the inbox and rename 
  data.file     = gsub('_ERROR_LOG' , '',  err.file.to.remove)
  
  file.copy(from = paste0("Y:/",wsp.fldr.name,"/ERROR/",data.file)
            ,to =  paste0("Y:/",wsp.fldr.name,"/INBOX/",data.file)
            ,overwrite = FALSE)
  # Delete data and error log files from the Error folder
  file.remove(c(
    paste0("Y:/",wsp.fldr.name,"/ERROR/",data.file))
    , paste0("Y:/",wsp.fldr.name,"/ERROR/",err.file)
  )  
}



