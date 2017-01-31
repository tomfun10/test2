library(data.table)

# Try to load contents of first file and catch errors 
fileCanBeRead = function (dirPath, nameOfFileToRead) { 
  file_to_read = paste0(dirPath,'/' ,nameOfFileToRead)
  rd = try(data.table::fread(file_to_read)) 
  
  # Catch file read errors 
  if ( 'try-error' %in% class(rd)) {
    msg='Err: Caught an error during file read\n'
    stop(msg) }
  else {
    msg='OK: file successfully read into var: rd\n'
    message(msg)
    return(data.table::fread(file_to_read))
  }
}

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
    msg='Err: file name columns have changed\n' 
    message(fileNamesTest[!(TRUE)])
    warning(msg)
  }
}

Expected_NameOfWSP <- function (Expected_NameOfWSP) {
  if (unique(toupper(rd$`Name of WSP`))== toupper(Expected_NameOfWSP )){
    message('OK: WSP name is consistent')
  }else {
    msg='Err: WSP name has changed'
    message(paste0('WSP Name supplied: ',unique(rd$`Name of WSP`,' Expected_NameOfWSP: ',Expected_NameOfWSP)))
    stop(msg)
  }
}

MakeSureDatesAreCorrect_dmy =  function(dateColToTest){
  result = try(lubridate::dmy(dateColToTest))
  if (class(result) ==  'Date') {
    message('Ok: All items are dates: ' )
    return(result)
  }else {
    msg = 'Err: All these are not dates '
    stop(msg)
  }
}

MakeSureDatesAreCorrect_ymd =  function(dateColToTest){
  result = try(lubridate::ymd(dateColToTest))
  if (class(result) ==  'Date') {
    message('Ok: All items are dates: ' )
    return(result)
  }else {
    msg = 'Err: All these are not dates '
    stop(msg)
  }
}

# Make sure Result Date is at or after Event Date
IsLaterDateAfterEarlierDate = function (FileToOpen, LaterDate,EarlierDate){

    if(sum(LaterDate >= EarlierDate ) == length(rd$`Bet Result Date`)){
    message('OK: All Bet Result Dates are on or after Event dates')
  }else{
    msg='Err: Not all Bet Result Dates are on or after Event dates'
    print(data.frame(LaterDate , EarlierDate)[ !(LaterDate >= EarlierDate) ,])
    shell.exec(FileToOpen) 
    stop(msg)
  }
}

# Make sure data to write back is only a data frame 
isObjectOnlyDataFrame = function(fileOjbect)
  if (is.data.frame(fileOjbect)){
    message('OK: data is only a data frame')
    TRUE
  }else{
    msg = 'Err: data is not only a data frame'
    stop(msg)
    FALSE}

# ---------------
# There must be at least 1 row's worth of data to read 

isObjectOnlyDataFrameAndAtLeastOneRowData = function(fileOjbect)
  if (is.data.frame(fileOjbect) & nrow(fileOjbect)>0 ){
    message("OK: data is only a data frame and hase at least 1 row of data")
    TRUE
  }else{
    stop("Err: data is not only a data frame and doesn't have more than 1 row of data")
    FALSE
    }

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

# Check expected data types 
number_of_cols = 16
CorrDataTypes = function(){
  CorrectDType=vector(mode = 'logical' , number_of_cols)  
  
  CorrectDType[1]  =  is.character(rd$name_of_wsp)
  CorrectDType[2]  =  DescTools::IsDate(rd$event_date)
  CorrectDType[3]  =  DescTools::IsDate(rd$bet_result_date)
  CorrectDType[4]  =  is.character(rd$venue)
  CorrectDType[5]  =  is.integer(as.integer(rd$race_number))
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
  CorrectDType[16] =  is.character(rd$dateTimeStamp_file_processed_grv)
  
  
  if (sum(CorrectDType) == number_of_cols ) {
      message('OK: Column names and data types are correct ')}
  else{   
      # message('Err: Column names and data types not as expected') 
      ColWithDataTypErr = which(CorrectDType  == FALSE)
      message()
      msg = paste0("Err: check column: ", ColWithDataTypErr )
      stop(msg)
    }
}

# Check TrackNames are consistent 

AnyUnknownVenueNames = function(FileToOpen, VenueNames){
knownVenueNames = c(  
  'Ballarat'
  ,'Bendigo'
  ,'Cranbourne'
  ,'Geelong'
  ,'Healesville'
  ,'Horsham'
  ,'Sale'
  ,'Sandown Park'
  ,'Shepparton'
  ,'The Meadows'
  ,'Traralgon'
  ,'Warragul'
  ,'Warrnambool'
)
if (length(VenueNames[!(VenueNames %in% knownVenueNames  )]) >0 ) {
  message('Err: Unkown Venue names')
  shell.exec(FileToOpen)
  stop(VenueNames[!(VenueNames %in% knownVenueNames  )])
} else {
  message('OK: All venue names are ok ')
}
}

# Known tracks, note there is no discrimintor for MEP and SAP ie the 2 new tracks
  # VenueNames[VenueNames == 'Ballarat']      <-	'BAL'
  # VenueNames[VenueNames == 'Bendigo']     <-		'BEN'
  # VenueNames[VenueNames == 'Cranbourne']  <-		'CRN'
  # VenueNames[VenueNames == 'Geelong']     <-		'GEL'
  # VenueNames[VenueNames == 'Healesville'] <-		'HVL'
  # VenueNames[VenueNames == 'Horsham']     <-		'HOR'
  # VenueNames[VenueNames == 'Sale']        <-		'SLE'
  # VenueNames[VenueNames == 'Sandown Park'] <-		'SAND'
  # VenueNames[VenueNames == 'Shepparton']  <-		'SHP'
  # VenueNames[VenueNames == 'The Meadows'] <-		'MEAD'
  # VenueNames[VenueNames == 'Traralgon']   <-		'TRA'
  # VenueNames[VenueNames == 'Warragul']    <-		'WGL'
  # VenueNames[VenueNames == 'Warrnambool'] <-		'WBL'
  
  # VenueNames  

insertFileIntoDbase = function (fileOject){
  library(RODBC)
  if (!(exists('fto')))   fto = odbcConnect("R_WH_Test_SQLNativeClient64Bit",uid="GRMS",pwd="Password1")

    rdFinal = fileOject[,c(
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
    ,'file_name'
    ,'date_file_processed_grv'
    ,'dateTimeStamp_file_processed_grv'
    )
    ]
    
  
    RODBC::sqlSave(channel = fto
                 ,dat = rdFinal
                 ,tablename = 'tb_daily_wsp_files_staging'
                 ,append = TRUE
                 ,rownames = FALSE
                 ,colnames = FALSE
                 ,verbose = TRUE
                 ,safer = TRUE 
                 ,addPK = FALSE
                 ,test = FALSE
  )
}  

sqlRunAnaySelectAgainstWareHouseTest = function(sql) {
  library(RODBC)
  if (!(exists('fto')))   fto = odbcConnect("R_WH_Test_SQLNativeClient64Bit",uid="GRMS",pwd="Password1")
  RODBC::sqlQuery(channel = fto, query = sql,errors = TRUE)
}

