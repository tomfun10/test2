# Problems: 
# non-utf8 encoding causes run-time errors
# Null values (instead of 0) endings on final columns causes run-time errors
# Check that data is written to tables but file is not transferred, thereby duplicating insertion of records in tables
# am not testing for valid race numbers ie 1-12, am getting 0 race numbers, associated with multi-leg races ?
# need to handle duplicate entries (ie duplicates before my final additional columns)
# Sandown instead of 'Sandown Park', there are venue names like 'Cranbourne Cup Heats', 'The Topgun' etc being used as Venue names, with eg race number = 0 .. 'VIC National Distance Final'
# sometimes 'Shepperaton' instead of 'Shepperton' , with 0 race number ... looks like some entries are hand written???
# no check for races at venue on a date
# -------------------------------------------------------------------------
# library(data.table);  library(RODBC) ; library(DescTools) 
 source('003 WSP_test_functions_v2.R')

i = 0
while(i<50) {
  print(paste0('Sequence: ', i ))
  source('H:\\___My_Folders\\00000 H_Drive Analyses\\227 WSP_detailed_audit\\WSP_detailed_Anal\\002 test_driver_v2.R')
  
i = i+1
}

