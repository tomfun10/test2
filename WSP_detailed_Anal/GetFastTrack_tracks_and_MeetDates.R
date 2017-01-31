library(RODBC);
sql= "
  SELECT 
  	meeting.MeetingDate
  	,track.TrackName
    ,TrackCode
  	,race.RaceNumber
  from 
  	race	
  	join meeting on race.PublishedMeeting_id = meeting.id 
  	join track on meeting.Track_id = track.id
  where 
  Meeting.MeetingDate between dateadd(month,-24,getdate()) and getdate()
  and meeting.IsQuali = 0
  order by meeting.MeetingDate, track.trackName, race.RaceNumber
"
fto = odbcConnect("ODBC64_FTrack_2016",uid="GRMS",pwd="Password1");
sql2 = gsub(sql,pattern = '\t|\n',replacement = ' ',ignore.case = TRUE );
dfMeets = sqlQuery(fto,sql2 , stringsAsFactors=FALSE)
RODBC::odbcClose(fto)

summary(dfMeets)

# ----------------------------------------------------------------- 
# To write to warehouse test data base.
  # 1) One line summary of file load
  # 2) Exhustive checked file loadings

str(rd)

tmp <- sqlColumns(fto, 'd1_Meeting')
tmp


RODBC::sqlSave(channel = fto, tablename = 'tb_daily_wsp_filesx' , dat = data.frame(format(rd[,2],"%Y-%m-%d")))

sql2 = gsub(sql,pattern = '\t|\n',replacement = ' ',ignore.case = TRUE );
dfMeets = sqlQuery(fto,sql2 , stringsAsFactors=FALSE)
RODBC::odbcClose(fto)
2) Exhaustive 


# cannot insert dates directly into SQL Azure directly from R RODBC
s tb_daily_wsp_filesx

select * from information_schema.columns where upper(column_name) like upper('event_date')

select * from tb_daily_wsp_filesx
drop table tb_daily_wsp_filesx

CREATE table tb_daily_wsp_files (
  id int identity(1,1)
  ,name_of_wsp varchar(100) 
  ,event_date date
  ,bet_result_date date
  ,venue varchar(50)
  ,race_number int
  ,pari_bets_taken money
  ,non_pari_bets_taken money
  ,bets_paid_credited_to_customers money
  ,net_customer_winnings money
  ,bets_back money
  ,bet_back_rev money
  ,other_rev money
  ,jackpots_created money
  ,[file_name] varchar(300)
  ,date_file_processed_grv date
  ,dateTimeStamp_file_processed_grv varchar(25)
)











