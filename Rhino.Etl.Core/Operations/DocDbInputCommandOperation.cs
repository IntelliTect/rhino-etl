using System.Configuration;
using Rhino.Etl.Core.Infrastructure;

namespace Rhino.Etl.Core.Operations
{
    using Microsoft.Azure.Documents.Linq;
    using Microsoft.Azure.Documents;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using DataReaders;

    /// <summary>
    /// Generic input command operation
    /// </summary>
    public class DocDbInputCommandOperation : AbstractDocumentDbOperation
    {
        /// <summary>
        /// The query to execute
        /// </summary>
        public string Command { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocDbInputCommandOperation"/> class.
        /// </summary>
        /// <param name="endpointUrlName">>Appsetting key for Url to DocumentDb endpoint.</param>
        /// <param name="endpointKeyName">>Appsetting key for Key for DocumentDB</param>
        /// /// <param name="documentDatabase">The Database to query</param>
        /// <param name="documentCollection">The DocumentCollection to query</param>
        public DocDbInputCommandOperation(string endpointUrlName, string endpointKeyName, string documentDatabase, string documentCollection)
            : base(endpointUrlName, endpointKeyName, documentDatabase, documentCollection)
        {
            UseTransaction = false;
        }

        /// <summary>
        /// Executes this operation
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <returns></returns>
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            IEnumerable<Database> dbQuery = from db in Client.CreateDatabaseQuery()
                                            where db.Id == DocumentDatabase
                                            select db;


            Database database = dbQuery.FirstOrDefault();

            var collection = Client.CreateDocumentCollectionQuery(database.CollectionsLink).Where(coll => coll.Id == DocumentCollection).AsEnumerable().FirstOrDefault();


            var query = Client.CreateDocumentQuery(collection.DocumentsLink, this.Command);
            EnumerableDataReader reader = new DocumentDbDataReader(query.AsEnumerable());
            while (reader.Read())
            {
                yield return CreateRowFromReader(reader);
            }


            //Client.CreateDocumentCollectionQuery(this.EndpointUrl, )

            // Client = new DocumentClient(new Uri(ConfigurationManager.AppSettings[EndpointUrl]),
            //    ConfigurationManager.AppSettings[EndpointKey],
            // new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp });

            /*
                using (currentCommand = connection.CreateCommand())
                {
                    currentCommand.Transaction = transaction;
                    PrepareCommand(currentCommand);
                    using (IDataReader reader = currentCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            yield return CreateRowFromReader(reader);
                        }
                    }
                }

                if (transaction != null) transaction.Commit();*/
        }

        /// <summary>
        /// Creates a row from the reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        protected Row CreateRowFromReader(IDataReader reader)
        {
            return Row.FromReader(reader);
        }
    }
}
