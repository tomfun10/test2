SELECT count(*) Cnt from dbo.tb_daily_wsp_files


SELECT top 20 *  from dbo.tb_daily_wsp_files


SELECT COUNT(*)
FROM (
    SELECT DISTINCT * FROM dbo.tb_daily_wsp_files
) T1