-- drop & recreate, since Synapse doesn’t support CREATE OR ALTER
IF OBJECT_ID('dbo.LogCohortCount','P') IS NOT NULL
    DROP PROCEDURE dbo.LogCohortCount;
GO

CREATE PROCEDURE dbo.LogCohortCount
    @CohortName  NVARCHAR(50),
    @CohortCount INT,
    @FactCount   INT,
    @ProfileName NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DBName NVARCHAR(20);
    SET @DBName = LEFT(DB_NAME(), 20);

    -- Upsert via EXISTS, since @@ROWCOUNT isn't supported
    IF EXISTS (
        SELECT 1
        FROM dbo.CohortCountLog
        WHERE CohortName = @CohortName
              AND ProfileName = @ProfileName
    )
    BEGIN
        UPDATE dbo.CohortCountLog
           SET CohortCount  = @CohortCount,
               FactCount    = @FactCount,
               UpdatedAt    = GETDATE(),
               DatabaseName = @DBName
         WHERE CohortName = @CohortName
               AND ProfileName = @ProfileName;
    END
    ELSE
    BEGIN
        INSERT INTO dbo.CohortCountLog (
            CohortName, CohortCount, FactCount, DatabaseName, ProfileName
        ) VALUES (
            @CohortName, @CohortCount, @FactCount, @DBName, @ProfileName
        );
    END
END
GO
