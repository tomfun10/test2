-- from: GRV_WarehouseTest

lf tb

sp_columns_100  tb_daily_wsp_files_staging

SELECT count(*) as Cnt from tb_daily_wsp_files_staging

SELECT count(DISTINCT tb_daily_wsp_files_staging.file_name) as CntUniqFilesRead from tb_daily_wsp_files_staging

SELECT * from tb_daily_wsp_files_staging

SELECT t.* into tb_daily_wsp_files_staging from (
SELECT * 
from tb_daily_wsp_files
) t 

