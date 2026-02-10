IF OBJECT_ID('dbo.ExecutionLog') IS NOT NULL
	DROP TABLE ExecutionLog;

IF OBJECT_ID('dbo.ExecutionLog','U') IS NULL
BEGIN
    CREATE TABLE dbo.ExecutionLog
    (
        RuleName        NVARCHAR(200)   NOT NULL,
        RuleGroup       NVARCHAR(50)    NOT NULL,
        ProfileName     NVARCHAR(50)    NULL,
        CycleId         UNIQUEIDENTIFIER NULL,
        IsSuccess       BIT             NOT NULL,
        Message         NVARCHAR(200)   NULL,
        StartTime       DATETIME2(7)    NOT NULL,
		EndTime	        DATETIME2(7)    NOT NULL,
		Duration        INT             NULL,
		DatabaseName	NVARCHAR(20)    NULL,
        LoadType        NVARCHAR(6)     NULL
    )
    WITH
    (
        DISTRIBUTION = HASH(RuleName),
        CLUSTERED COLUMNSTORE INDEX
    );
END;


IF OBJECT_ID('dbo.CohortCountLog') IS NOT NULL
	DROP TABLE CohortCountLog;

IF OBJECT_ID('dbo.CohortCountLog','U') IS NULL
BEGIN
    CREATE TABLE dbo.CohortCountLog
    (
        CohortName      NVARCHAR(50)    NOT NULL,
        CohortCount     INT             NULL,
        FactCount       INT             NULL,
        UpdatedAt       DATETIME2(7)    NOT NULL,
		DatabaseName    NVARCHAR(20)    NULL,
        ProfileName     NVARCHAR(50)    NULL
    )
    WITH
    (
        DISTRIBUTION = HASH(CohortName),
        CLUSTERED COLUMNSTORE INDEX
    );
END;
