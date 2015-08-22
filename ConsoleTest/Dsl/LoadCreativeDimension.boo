import System
operation map_columns:
    for row in rows:
        continue if row.CreativeId is null
        row.MBS_Creative_ExternalId = row.CreativeId
        yield row
    
process LoadCreativeDimension:
    input "MBS", Command = """
    SELECT TOP 100
	CONVERT(nvarchar(255), CO.CommercialId) AS CreativeId
	,CA.ClientAccountName as ClientName
	,CC.CreativeClassName AS MediaType
	,CO.CommercialTitle AS Description
	,PC.ConstituentName AS ProductionCompanyName
	,CO.ExpireDt AS ExpireDate
FROM dbo.Commercial AS CO
	JOIN dbo.Constituent AS C ON (C.ConstituentId = CO.ClientId)
		AND dbo.IsValidGUID(C.ConstituentId) = 1
		AND C.ConstituentTypeNbr = 1
	JOIN dbo.CreativeClass AS CC ON (CC.CreativeClassType = CO.CreativeClassType)
	JOIN dbo.CommercialType AS CT ON (CT.CommercialTypeNbr = CO.CommercialTypeNbr)
	LEFT JOIN dbo.CreativeGroup AS CG ON (CG.CreativeGroupId = CO.CreativeGroupId)
	LEFT JOIN dbo.CallCenter AS CC2 ON (CC2.CallCenterId = CO.CallCenterId)
	LEFT JOIN dbo.Curriculum AS CR ON (CR.CurriculumId = CO.CurriculumId)
	LEFT JOIN dbo.Constituent AS DH ON (DH.ConstituentId = CO.DubHouseId)
		AND DH.ConstituentTypeNbr = 4
	LEFT JOIN dbo.Constituent AS PC ON (PC.ConstituentId = CO.ProdCoId)
		AND PC.ConstituentTypeNbr = 5
	LEFT JOIN dbo.ClientAccount CA ON CA.ClientAccountId = C.ClientAccountId
    """
    map_columns()
    sqlBulkInsert "DW", "Stage_Creative", PreCommand = """
    CREATE TABLE Stage_Creative (
	[ClientName] [nvarchar](100),
	[MediaType] [nvarchar](50),
	[Description] [nvarchar](255),
	[ProductionCompanyName] [nvarchar](255),
	[ExpireDate] [datetimeoffset](7),
	[CreatedDateTimeStamp] [datetimeoffset](7),
	[ModifiedDateTimeStamp] [datetimeoffset](7),
	[MBS_Creative_ExternalId] [nvarchar](255));
    """, PostCommand = """
    MERGE Creative AS TARGET
    USING Stage_Creative AS SOURCE
    ON (TARGET.MBS_Creative_ExternalId = SOURCE.MBS_Creative_ExternalId)
    WHEN MATCHED AND (
     TARGET.ClientName <> SOURCE.ClientName OR 
     TARGET.MediaType <> SOURCE.MediaType OR
     TARGET.Description <> SOURCE.Description OR
     TARGET.ProductionCompanyName <> SOURCE.ProductionCompanyName OR
     TARGET.ExpireDate <> SOURCE.ExpireDate)
     THEN UPDATE SET 
          TARGET.ClientName = SOURCE.ClientName, 
          TARGET.MediaType = SOURCE.MediaType,
          TARGET.Description = SOURCE.Description,
          TARGET.ProductionCompanyName = SOURCE.ProductionCompanyName,
          TARGET.ExpireDate = SOURCE.ExpireDate,
          TARGET.ModifiedDateTimeStamp = GetDate()
    WHEN NOT MATCHED BY TARGET THEN 
     INSERT (ClientName, MediaType, Description, ProductionCompanyName, ExpireDate, CreatedDateTimeStamp, ModifiedDateTimeStamp,  MBS_Creative_ExternalId) 
     VALUES(SOURCE.ClientName, SOURCE.MediaType, SOURCE.Description, SOURCE.ProductionCompanyName, SOURCE.ExpireDate, GetDate(), GetDate(), SOURCE.MBS_Creative_ExternalId)
    WHEN NOT MATCHED BY SOURCE THEN DELETE;  DROP TABLE Stage_Creative;
    """ :
        map "ClientName"
        map "MediaType"
        map "Description"
        map "ProductionCompanyName"
        map "ExpireDate"
        map "CreatedDateTimeStamp"
        map "ModifiedDateTimeStamp"
        map "MBS_Creative_ExternalId"