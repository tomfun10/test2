# Use data warehouse 

SELECT * from tb_daily_wsp_files_staging

SELECT * from tb_daily_wsp_files

drop table tb_daily_wsp_files

CREATE table        tb_daily_wsp_files (
  name_of_wsp				varchar(50)
  ,event_date					date
  ,bet_result_date			date
  ,venue						varchar(50)
  ,race_number				int
  ,pari_bets_taken			float
  ,non_pari_bets_taken		float
  ,bets_paid_credited_to_customers		float
  ,net_customer_winnings		float
  ,bets_back		            float
  ,bet_back_rev	        	float
  ,other_rev		            float
  ,jackpots_created	     	float
  ,[file_name]		        varchar(100)
  ,date_file_processed_grv	date
  ,dateTimeStamp_file_processed_grv		varchar(100)
)