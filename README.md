**APPROACH**

March Easter Dates:
    Extract March month from relevant cell.
    Extract March days from the relevant row and columns - unpivot
    Extract March Years from the rows and columns
    Create a CTE (march_table) for March month.
    Create a CTE (march_year_table) with years and corresponding column names for Easter Sunday data.
    Create a CTE (MARCH_EASTERS) to identify Easter Sundays in March and their dates.

April Easter Dates:

    Perform similar steps as above for April month, creating CTEs (april_table, april_year_table, and APRIL_EASTERS).

Final Easter Dates Table:

    Combine March and April Easter dates using UNION.
    Filter data for years between 1700 and 2023.
    Select and format final date column (EASTER_SUNDAY).

**SCRIPT**

March cell value CTE
```sql
WITH march_table (MMMM) AS (
  SELECT 
    Top 1 REPLACE(X_3, ' ') as MMMM 
  FROM 
    TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK15_EASTER_DATES 
  WHERE 
    MMMM != '' // CTE of just March to cross 
    join to March headers
)
```
March Year Data CTE
```sql
march_year_table (YYYY, joinCol) AS (
  SELECT 
    YYYY, 
    col as joinCOL 
  FROM 
    (
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
      LIMIT 
        27 OFFSET 3
    ) UNPIVOT(
      YYYY FOR year.col IN (
        CONVERT, X_4, X_5, X_6, X_7, X_8, X_9, 
        X_10, X_11, X_12
      )
    )
) 
```

March Days from the particular row unpivoted and joined to the year data on the column name - month cross-joined to all records and then parse those 3 columns into a date.
```sql
MARCH_EASTERS (EASTER_SUNDAY) AS (
    SELECT 
      TO_DATE(
        CONCAT(
          TO_VARCHAR(DDD), 
          '/', 
          TO_VARCHAR(MMMM), 
          '/', 
          TO_VARCHAR(YYYY)
        ), 
        'DD/MMMM/YYYY'
      ) 
    FROM 
      (
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
        LIMIT 
          1 OFFSET 2
      ) UNPIVOT(
        ddd FOR day.col IN (
          CONVERT, X_4, X_5, X_6, X_7, X_8, X_9, 
          X_10, X_11, X_12
        )
      ) CROSS 
      JOIN march_table 
      INNER JOIN march_year_table ON COL = march_year_table.joinCOL
  )
```

Then repeat the process for April
```sql
 april_table (MMMM) AS (
    SELECT 
      Top 1 REPLACE(X_13, ' ') as MMMM 
    FROM 
      TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK15_EASTER_DATES 
    WHERE 
      MMMM != '' // CTE of just April to cross 
      join to April headers
  ), 
  April_year_table (YYYY, joinCol) AS (
    SELECT 
      YYYY, 
      col as joinCOL 
    FROM 
      (
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
        LIMIT 
          30 OFFSET 3
      ) UNPIVOT(
        YYYY FOR year.col IN (
          CONVERT, X_14, X_15, X_16, X_17, X_18, 
          X_19, X_20, X_21, X_22, X_23, X_24, 
          X_25, X_26, X_27, X_28, X_29, X_30, 
          X_31, X_32, X_33, X_34, X_35, X_36, 
          X_37
        )
      )
  ) // Captures all the years with April Easter Sundays 
  and captures the column it was stored in to be joined with the column header that houses the day 
values 
  ., 
  APRIL_EASTERS (EASTER_SUNDAY) AS (
    SELECT 
      TO_DATE(
        CONCAT(
          TO_VARCHAR(DDD), 
          '/', 
          TO_VARCHAR(MMMM), 
          '/', 
          TO_VARCHAR(YYYY)
        ), 
        'DD/MMMM/YYYY'
      ) 
    FROM 
      (
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
        LIMIT 
          1 OFFSET 2
      ) UNPIVOT(
        ddd FOR day.col IN (
          CONVERT, X_14, X_15, X_16, X_17, X_18, 
          X_19, X_20, X_21, X_22, X_23, X_24, 
          X_25, X_26, X_27, X_28, X_29, X_30, 
          X_31, X_32, X_33, X_34, X_35, X_36, 
          X_37
        )
      ) CROSS 
      JOIN april_table 
      INNER JOIN april_year_table ON col = april_year_table.joinCOL
  )
```

Final Part of the Query is to Union March and April and Filter based on challenge requirements
```sql
SELECT 
  * 
FROM 
  MARCH_EASTERS 
WHERE 
  YEAR(EASTER_SUNDAY) >= 1700 
  AND YEAR(EASTER_SUNDAY) < 2024 
UNION 
SELECT 
  * 
FROM 
  APRIL_EASTERS 
WHERE 
  YEAR(EASTER_SUNDAY) >= 1700 
  AND YEAR(EASTER_SUNDAY) < 2024;
// 324 Rows excluding headers = SUCCESS !
```
