-- LogExecutionStart
-- drop & recreate, since Synapse doesn’t support CREATE OR ALTER
IF OBJECT_ID('dbo.LogExecutionStart','P') IS NOT NULL
    DROP PROCEDURE dbo.LogExecutionStart;
GO

CREATE PROCEDURE dbo.LogExecutionStart
    @RuleName   SYSNAME,
    @RuleGroup  SYSNAME,
    @ProfileName NVARCHAR(50),
    @CycleId UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @Now      DATETIME,
        @DbName   SYSNAME,
        @LoadType NVARCHAR(50);
    SET @Now    = GETDATE();
    SET @DbName = DB_NAME();

    -- Check parameter table existence
    IF OBJECT_ID('IntConfig_Parameter','U') IS NULL
        SET @LoadType = 'Deploy';
    ELSE
        SELECT TOP 1 @LoadType = ParameterValue
        FROM IntConfig_Parameter
       WHERE SK_IntConfig_Parameter = 1;

    -- Upsert start entry via EXISTS
    -- IF EXISTS (SELECT 1 FROM dbo.ExecutionLog WHERE RuleName = @RuleName)
    -- BEGIN
    --     UPDATE dbo.ExecutionLog
    --        SET RuleGroup    = @RuleGroup,
    --            IsSuccess    = 0,
    --            Message      = 'Not run',
    --            StartTime    = @Now,
    --            EndTime      = @Now,
    --            Duration     = NULL,
    --            DatabaseName = @DbName,
    --            LoadType     = @LoadType
    --      WHERE RuleName = @RuleName;
    -- END
    -- ELSE
    -- BEGIN

    INSERT INTO dbo.ExecutionLog (
        RuleName, RuleGroup, ProfileName, CycleId, IsSuccess, Message,
        StartTime, EndTime, Duration, DatabaseName, LoadType
    ) VALUES (
        @RuleName, @RuleGroup, @ProfileName, @CycleId, 0, 'Started',
        @Now, @Now, NULL, @DbName, @LoadType
    );
    -- END
END
GO
