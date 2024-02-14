//Look at the Data

SELECT * 
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK15_EASTER_DATES;

//Probably need to make CTEs and then insert data into a pivotted structure - once for March and Once for April

WITH 
    march_table (MMMM) AS (
SELECT Top 1 REPLACE(X_3, ' ') as MMMM
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK15_EASTER_DATES
WHERE MMMM != ''
// CTE of just March to cross join to March headers
)
,
    march_year_table (YYYY,joinCol) AS (
SELECT YYYY, col as joinCOL
FROM (SELECT 
    TRY_TO_NUMBER(X_3) as CONVERT,
    X_4,
    X_5,
    X_6,
    X_7,
    X_8,
    X_9,
    X_10,
    X_11,
    X_12
    FROM
        TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK15_EASTER_DATES
    LIMIT 27 OFFSET 3 ) 
    UNPIVOT(YYYY FOR year.col IN (    
    CONVERT,
    X_4,
    X_5,
    X_6,
    X_7,
    X_8,
    X_9,
    X_10,
    X_11,
    X_12
    ))
    )
//Captures all the years with March Easter Sundays and captures the column it was stored in to be joined with the column header that houses the day values.
,
    MARCH_EASTERS (EASTER_SUNDAY) AS (
SELECT TO_DATE(CONCAT(TO_VARCHAR(DDD), '/',TO_VARCHAR(MMMM),'/',TO_VARCHAR(YYYY)), 'DD/MMMM/YYYY')
FROM (
    SELECT 
    TRY_TO_NUMBER(X_3) as CONVERT,
    X_4,
    X_5,
    X_6,
    X_7,
    X_8,
    X_9,
    X_10,
    X_11,
    X_12
    FROM
        TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK15_EASTER_DATES
    LIMIT 1 OFFSET 2  )
UNPIVOT(ddd FOR day.col IN (    
    CONVERT,
    X_4,
    X_5,
    X_6,
    X_7,
    X_8,
    X_9,
    X_10,
    X_11,
    X_12)
    )
    //Extracts the column headers - day of the month and pivots them with the column header to be joined to the month and years
CROSS JOIN march_table
INNER JOIN march_year_table
    ON COL = march_year_table.joinCOL
)
,
//April
april_table (MMMM) AS (
SELECT Top 1 REPLACE(X_13, ' ') as MMMM
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK15_EASTER_DATES
WHERE MMMM != ''
// CTE of just April to cross join to April headers
)
,
    April_year_table (YYYY,joinCol) AS (
SELECT YYYY, col as joinCOL
FROM (SELECT 
    TRY_TO_NUMBER(X_13) as CONVERT,
    X_14,
    X_15,
    X_16,
    X_17,
    X_18,
    X_19,
    X_20,
    X_21,
    X_22,
    X_23,
    X_24,
    X_25,
    X_26,
    X_27,
    X_28,
    X_29,
    X_30,
    X_31,
    X_32,
    X_33,
    X_34,
    X_35,
    X_36,
    X_37
    FROM
        TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK15_EASTER_DATES
    LIMIT 30 OFFSET 3 ) 
    UNPIVOT(YYYY FOR year.col IN (    
    CONVERT,
    X_14,
    X_15,
    X_16,
    X_17,
    X_18,
    X_19,
    X_20,
    X_21,
    X_22,
    X_23,
    X_24,
    X_25,
    X_26,
    X_27,
    X_28,
    X_29,
    X_30,
    X_31,
    X_32,
    X_33,
    X_34,
    X_35,
    X_36,
    X_37
    ))
    )
//Captures all the years with April Easter Sundays and captures the column it was stored in to be joined with the column header that houses the day values.
,
    APRIL_EASTERS (EASTER_SUNDAY) AS (
SELECT TO_DATE(CONCAT(TO_VARCHAR(DDD), '/',TO_VARCHAR(MMMM),'/',TO_VARCHAR(YYYY)), 'DD/MMMM/YYYY')
FROM (
    SELECT 
    TRY_TO_NUMBER(X_13) as CONVERT,
    X_14,
    X_15,
    X_16,
    X_17,
    X_18,
    X_19,
    X_20,
    X_21,
    X_22,
    X_23,
    X_24,
    X_25,
    X_26,
    X_27,
    X_28,
    X_29,
    X_30,
    X_31,
    X_32,
    X_33,
    X_34,
    X_35,
    X_36,
    X_37
    FROM
        TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK15_EASTER_DATES
    LIMIT 1 OFFSET 2  )
UNPIVOT(ddd FOR day.col IN (    
    CONVERT,
    X_14,
    X_15,
    X_16,
    X_17,
    X_18,
    X_19,
    X_20,
    X_21,
    X_22,
    X_23,
    X_24,
    X_25,
    X_26,
    X_27,
    X_28,
    X_29,
    X_30,
    X_31,
    X_32,
    X_33,
    X_34,
    X_35,
    X_36,
    X_37)
    )
CROSS JOIN april_table
INNER JOIN april_year_table
    ON col = april_year_table.joinCOL
)
//Final staging table for both months - find the days contained in each column and pivot long - join month to every row associated with that month with cross join and then join the years if the column header value matches X_3, X_4 etc. Select the YYYY MMMM DD values and turn into a date
//MARCH COMPLETE - DO SAME FOR APRIL THEN IN QUERY UNION THE TWO
SELECT *
FROM MARCH_EASTERS
WHERE
YEAR(EASTER_SUNDAY) >= 1700 AND
YEAR(EASTER_SUNDAY) < 2024
UNION 
SELECT *
FROM APRIL_EASTERS
WHERE YEAR(EASTER_SUNDAY) >= 1700 AND
YEAR(EASTER_SUNDAY) < 2024
;
//324 Rows excluding headers = SUCCESS!