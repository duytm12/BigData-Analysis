# First, we create a table in Data Warehouse to store Result from ETL process
DROP TABLE IF EXISTS datawarehouse.log_content_olap;
CREATE TABLE datawarehouse.log_content_olap (
    Contract VARCHAR(100) PRIMARY KEY,
    ChildDuration BIGINT,
    MovieDuration BIGINT,
    SportDuration BIGINT,
    TVDuration BIGINT,
    RelaxDuration BIGINT,
    MostWatch VARCHAR(50),
    Taste VARCHAR(150),
    DateCount MEDIUMINT,
    Activeness DECIMAL (5,2));

# Second, we create a table in Data Mart for ELT purpose
DROP TABLE IF EXISTS datamart.daily_statistics;
CREATE TABLE datamart.log_content_olap (
    TotalUsers VARCHAR(100) PRIMARY KEY,
    ChildDuration BIGINT,
    MovieDuration BIGINT,
    SportDuration BIGINT,
    TVDuration BIGINT,
    RelaxDuration BIGINT);

# ELT process: Get Daily Statistics from Data Warehouse
CREATE PROCEDURE insert_daily_statistics()
BEGIN
    DECLARE date_var DATE;
    SET date_var = CURDATE();

    INSERT INTO datamart.daily_statistics (Date, TotalUser, ChildDuration, MovieDuration, SportDuration, TVDuration, RelaxDuration)
    SELECT Date, TotalUser, ChildDuration, MovieDuration, SportDuration, TVDuration, RelaxDuration
    FROM (
        SELECT date_var AS Date,
               COUNT(Contract) AS TotalUser,
               SUM(ChildDuration) AS ChildDuration,
               SUM(MovieDuration) AS MovieDuration,
               SUM(SportDuration) AS SportDuration,
               SUM(TVDuration) AS TVDuration,
               SUM(RelaxDuration) AS RelaxDuration
        FROM datawarehouse.log_content_olap
        WHERE DATE(log_content_olap.DateCount) = date_var
        GROUP BY date_var
    ) AS subquery;
END;

# Set Job Scheduler for automation
CREATE EVENT auto_insert_daily_statistics ON SCHEDULE
    EVERY '1' day
    STARTS  '2024-05-11 00:00:00'
    ON COMPLETION PRESERVE
    ENABLE
    DO
    CALL insert_daily_statistics();