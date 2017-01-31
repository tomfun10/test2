# Read and transfer one file at a time
library(DescTools)
options(scipen = 100) # Ensure non-scientific display of numbers 

getFilesFromDir    = 'C:/SPORTSBET daily wagering files/ProcessedAll' #get SportsBet processed files from this dir
moveCleansedToDir  =  'C:/SPORTSBET daily wagering files/ProcessedAll_afterR'                                                #once files have been processed and writtent to warehouse test dbase, move originating file to 

# get master list of processed files and sort by alphabetical order
d = dir(path = getFilesFromDir , pattern = '.CSV',ignore.case = TRUE,all.files = FALSE,include.dirs = FALSE) 
d = sort(d)

# Read directory for files and get first file
length(d)  # Total files to process 
d[1]       # name of first file to process

# Make avail functions to test this data 
source('WSP_test_functions.R')

# Read directory for files and get first file
rd = fileCanBeRead(getFilesFromDir, d[1])

# Structure of data before any cleanup 
str(rd)  #If read file successfully what does the structure look like ?

Expected_NameOfWSP('Sportsbet')   # WSP is consistent in how they identify themselves by name 
fNameTest(rd)                     # Is WSP being consistent in using Column names ?

# Write file Name and Date file processed by GRV to data supplied
rd$file_name = d[1]
rd$date_file_processed_grv            = lubridate::ymd(Sys.Date())
rd$dateTimeStamp_file_processed_grv  = as.character(date())

# Make sure all Dates are the correct data type
rd$`Event Date`                        =  MakeSureDatesAreCorrect_dmy(rd$`Event Date`)
rd$`Bet Result Date`                   =  MakeSureDatesAreCorrect_dmy(rd$`Bet Result Date`)
rd$date_file_processed_grv             =  MakeSureDatesAreCorrect_ymd(rd$date_file_processed_grv)
if (is.character(rd$dateTimeStamp_file_processed_grv)){message('OK: dateTimeStamp_file_processed_grv is character')} else{
                                                       message('Err: dateTimeStamp_file_processed_grv is not character')}

# Are all BetResults Date after Event Date ?
IsLaterDateAfterEarlierDate(rd$`Bet Result Date` , rd$`Event Date` )

# Check Venue names , note MEP and SAP have predate the Data Spec.
AnyUnknownVenueNames(rd$Venue)

# Is data object a data frame only, not some structure coerced ?
isObjectOnlyDataFrame(rd)   #Should be *TRUE*

isObjectOnlyDataFrameAndAtLeastOneRowData(rd) # Is there at least one row of data ?
names(rd)[1:13] = ConsistantColNames() # Rename the first 13 columns in a consistent way (last 2 cols are added by this script), ie *file_name* and *rd$date_file_processed_grv*

# coerce to correct data types 
rd$pari_bets_taken                  =  as.double(rd$pari_bets_taken)
rd$non_pari_bets_taken              =  as.double(rd$non_pari_bets_taken)
rd$bets_paid_credited_to_customers  =  as.double(rd$bets_paid_credited_to_customers) 
rd$net_customer_winnings            =  as.double(rd$net_customer_winnings)
rd$bets_back                        =  as.double(rd$bets_back)
rd$bet_back_rev                     =  as.double(rd$bet_back_rev)
rd$other_rev                        =  as.double(rd$other_rev)
rd$jackpots_created                 =  as.double(rd$jackpots_created)

# Check we've got all the expected data types
CorrDataTypes()

# Check: if Event date, Track, race number are the same update else insert

# Insert data into data base 


insertFileIntoDbase(rd)

# if Write to dbase successfull then move csv file 
file.rename(
     from = paste0(getFilesFromDir, "/", d[1])
    ,to   = paste0(moveCleansedToDir, "/", d[1])
)

sqlRunAnaySelectAgainstWareHouseTest("select count(*) Nrows from tb_daily_wsp_files")

