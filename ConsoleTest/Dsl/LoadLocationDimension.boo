operation map_columns:
    for row in rows:
        continue if row.LocationId is null
        row.ConstituentName = row.LocationName
        row.MBS_Campus_ExternalId = row.LocationId
        yield row
    
process LoadLocationDimension:
    input "MBS", Command = "SELECT CONVERT(nvarchar(255),C.ConstituentId) AS LocationId, C.ConstituentName AS LocationName,CT.ClientTypeName AS LocationType FROM dbo.Constituent AS C JOIN dbo.Client AS CL ON CL.ClientId = C.ConstituentId AND C.Hidden = 0 AND dbo.IsValidGUID(C.ConstituentId) = 1 JOIN dbo.ClientType AS CT ON CT.ClientTypeNbr = CL.ClientTypeNbr"
    map_columns()
    sqlBulkInsert "DW", "Stage_Location", PreCommand = """
    CREATE TABLE Stage_Location (ConstituentName nvarchar(255), MBS_Campus_ExternalId nvarchar(255));
    """, PostCommand = """
    MERGE Location AS TARGET
    USING Stage_Location AS SOURCE
    ON (TARGET.MBS_Campus_ExternalId = SOURCE.MBS_Campus_ExternalId)
    WHEN MATCHED THEN UPDATE SET TARGET.ConstituentName = SOURCE.ConstituentName
    WHEN NOT MATCHED BY TARGET THEN INSERT (ConstituentName, MBS_Campus_ExternalId) VALUES(SOURCE.ConstituentName, SOURCE.MBS_Campus_ExternalId)
    WHEN NOT MATCHED BY SOURCE THEN DELETE; DROP TABLE Stage_Location;
    """ :
        map "ConstituentName"
        map "MBS_Campus_ExternalId"

/*operation map_columns:
    for row in rows:
        continue if row.LocationId is null
        row.ConstituentName = row.LocationName
        row.MBS_Campus_ExternalId = row.LocationId
        yield row
    
process LoadLocationDimension:
    input "MBS", Command = "SELECT CONVERT(nvarchar(255),C.ConstituentId) AS LocationId, C.ConstituentName AS LocationName,CT.ClientTypeName AS LocationType FROM dbo.Constituent AS C JOIN dbo.Client AS CL ON CL.ClientId = C.ConstituentId AND C.Hidden = 0 AND dbo.IsValidGUID(C.ConstituentId) = 1 JOIN dbo.ClientType AS CT ON CT.ClientTypeNbr = CL.ClientTypeNbr"
    map_columns()
    output "DW", PreCommand = """
    CREATE TABLE Stage_Location (ConstituentName nvarchar(255), MBS_Campus_ExternalId nvarchar(255));
    """, Command = """
    INSERT INTO Stage_Location (ConstituentName, MBS_Campus_ExternalId)
    VALUES (@ConstituentName, @MBS_Campus_ExternalId)
    """, PostCommand = """
    MERGE Location AS TARGET
    USING Stage_Location AS SOURCE
    ON (TARGET.MBS_Campus_ExternalId = SOURCE.MBS_Campus_ExternalId)
    WHEN MATCHED THEN UPDATE SET TARGET.ConstituentName = SOURCE.ConstituentName
    WHEN NOT MATCHED BY TARGET THEN INSERT (ConstituentName, MBS_Campus_ExternalId) VALUES(SOURCE.ConstituentName, SOURCE.MBS_Campus_ExternalId)
    WHEN NOT MATCHED BY SOURCE THEN DELETE; DROP TABLE Stage_Location;
    """*/


    /*
    operation map_columns:
    for row in rows:
        continue if row.LocationId is null
        row.ConstituentName = row.LocationName
        row.MBS_Campus_ExternalId = row.LocationId
        yield row
    
process LoadLocationDimension:
    input "MBS", Command = "SELECT  CONVERT(nvarchar(255),C.ConstituentId) AS LocationId,C.ConstituentName AS LocationName,CT.ClientTypeName AS LocationType FROM dbo.Constituent AS C JOIN dbo.Client AS CL ON CL.ClientId = C.ConstituentId AND C.Hidden = 0 AND dbo.IsValidGUID(C.ConstituentId) = 1 JOIN dbo.ClientType AS CT ON CT.ClientTypeNbr = CL.ClientTypeNbr"
    map_columns()
    output "DW", Command = """
    INSERT INTO Location (ConstituentName, MBS_Campus_ExternalId)
    VALUES (@ConstituentName, @MBS_Campus_ExternalId)
    """
    */