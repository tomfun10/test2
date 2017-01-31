da =lubridate::ymd( 
  '2017-01-01',
  '2017-01-01',
  '2017-01-01',
  '2017-01-01',         
  '2017-01-02',
  '2017-01-02',
  '2017-01-03'
)                      
trk = as.character(c(
  'san',
  'mea',
  'san',
  'crn',
  'crn',
  'crn',             
  'wbl'
))

dff = data.frame(da,trk, stringsAsFactors = FALSE)  
str(dff)

ff = function(dd, aDate) dd[da == as.Date(aDate),]
'mea' %in% unique(ff(dff,'2017-01-01')$trk)

