----------------------------- LogExecutionResult
-- drop & recreate
IF OBJECT_ID('dbo.LogExecutionResult','P') IS NOT NULL
    DROP PROCEDURE dbo.LogExecutionResult;
GO


CREATE PROCEDURE [dbo].[LogExecutionResult]
  @RuleName     SYSNAME,
  @IsSuccess    BIT,
  @ErrorMessage NVARCHAR(4000),
  @ProfileName  NVARCHAR(50),
  @CycleId UNIQUEIDENTIFIER = NULL
AS
BEGIN
  SET NOCOUNT ON;

  DECLARE @EndTime DATETIME2(7) = GETDATE();
  DECLARE @StartTime DATETIME2(7);

  -- pull the StartTime you wrote in LogExecutionStart
  SELECT @StartTime = StartTime
  FROM dbo.ExecutionLog
  WHERE RuleName = @RuleName
        AND ProfileName = @ProfileName
        AND ((@CycleId IS NULL AND CycleId IS NULL) OR CycleId = @CycleId)
        AND StartTime = (
                SELECT MAX(StartTime)
                FROM   dbo.ExecutionLog
                WHERE  RuleName = @RuleName
                       AND ProfileName = @ProfileName
                       AND ((@CycleId IS NULL AND CycleId IS NULL) OR CycleId = @CycleId)
            );
   -- Only update if we found a matching row
   IF @StartTime IS NOT NULL
   BEGIN
    UPDATE dbo.ExecutionLog
        SET IsSuccess		= @IsSuccess,
            Message		= COALESCE(@ErrorMessage,           -- if error, show message
                                    CASE WHEN @IsSuccess = 1 
                                        THEN 'Completed' 
                                        ELSE 'Failed' END),
            EndTime		= @EndTime,
            Duration		= DATEDIFF(SECOND, @StartTime, @EndTime)
    WHERE RuleName = @RuleName
    AND ProfileName = @ProfileName
    AND ((@CycleId IS NULL AND CycleId IS NULL) OR CycleId = @CycleId)
    AND StartTime = @StartTime;
   END
END;
GO

