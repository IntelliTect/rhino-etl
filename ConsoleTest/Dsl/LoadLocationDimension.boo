operation map_columns:
    for row in rows:
        continue if row.LocationId is null
        row.ConstituentName = row.LocationName
        row.MBS_Campus_ExternalId = row.LocationId
        yield row
    
process LoadLocationDimension:
    input "MBS", Command = "SELECT  CONVERT(nvarchar(255),C.ConstituentId) AS LocationId,C.ConstituentName AS LocationName,CT.ClientTypeName AS LocationType FROM dbo.Constituent AS C JOIN dbo.Client AS CL ON CL.ClientId = C.ConstituentId AND C.Hidden = 0 AND dbo.IsValidGUID(C.ConstituentId) = 1 JOIN dbo.ClientType AS CT ON CT.ClientTypeNbr = CL.ClientTypeNbr"
    map_columns()
    sqlBulkInsert "DW", "Location" :
        map "ConstituentName"
        map "MBS_Campus_ExternalId"