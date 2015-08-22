import System
operation map_columns:
    for row in rows:
        continue if row.LocationId is null
        row.ClientName = row.ClientName
        row.MBS_Client_ExternalId = row.ClientId
        row.CreatedDateTime = DateTime.Now
        row.ModifiedDateTime = DateTime.Now
        yield row
    
process LoadClientDimension:
    input "MBS", Command = "SELECT CONVERT(nvarchar(255),CA.ClientAccountId) AS ClientId, CA.ClientAccountName AS ClientName FROM dbo.ClientAccount AS CA"
    map_columns()
    sqlBulkInsert "DW", "Stage_Client", PreCommand = """
    CREATE TABLE Stage_Client (ClientName nvarchar(255), MBS_Client_ExternalId nvarchar(255), CreatedDateTime datetime, ModifiedDateTime datetime);
    """, PostCommand = """
    MERGE Client AS TARGET
    USING Stage_Client AS SOURCE
    ON (TARGET.MBS_Client_ExternalId = SOURCE.MBS_Client_ExternalId)
    WHEN MATCHED AND TARGET.ClientName <> SOURCE.ClientName THEN UPDATE SET TARGET.ClientName = SOURCE.ClientName, TARGET.ModifiedDateTime = GetDate()
    WHEN NOT MATCHED BY TARGET THEN INSERT (ClientName, MBS_Client_ExternalId, CreatedDateTime, ModifiedDateTime) VALUES(SOURCE.ClientName, SOURCE.MBS_Client_ExternalId, GetDate(), GetDate())
    WHEN NOT MATCHED BY SOURCE THEN DELETE; 
    """ :
        map "ClientName"
        map "MBS_Client_ExternalId"
        map "CreatedDateTime"
        map "ModifiedDateTime"