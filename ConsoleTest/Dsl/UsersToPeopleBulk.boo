operation split_name_bulk:
    for row in rows:
        continue if row.Name is null
        row.FirstName = row.Name.Split()[0]
        row.LastName = row.Name.Split()[1]
        yield row
    
process UsersToPeopleBulk:
    DocDbInput "AzureDocumentDbEndpointUrl", "AzureDocumentDbEndpointKey", "mpm-files", "ImportedFiles", Command = "SELECT 'my name' as Name, 12 as id, 'test' as firstname, 'test' as lastname, 'abc@abc.com' as email FROM c"
    split_name_bulk()
    sqlBulkInsert "RhinoTest", "People", TableLock = true :
        map "id", int
        map "firstname"
        map "lastname"
        map "email"
        map "userid", "id", int