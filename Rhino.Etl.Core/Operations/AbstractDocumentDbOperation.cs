using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Rhino.Etl.Core.Operations
{
    /// <summary>
    /// Represent an operation that uses Azure DocumentDB that can occur during the ETL process
    /// </summary>
    public abstract class AbstractDocumentDbOperation : AbstractOperation
    {
        /// <summary>
        /// Gets or sets the DocumentDb client
        /// </summary>
        protected DocumentClient Client {get; set;}

        /// <summary>
        /// The database to query
        /// </summary>
        protected string DocumentDatabase { get; set; }

        /// <summary>
        /// The DocumentCollection to query
        /// </summary>
        protected string DocumentCollection { get; set; }

        /// <summary>
        /// The Azure Endpoint Url for the documentDB
        /// </summary>
        protected string EndpointUrl { get; set; }

        /// <summary>
        /// The Azure Endpoint Key for the documentDB
        /// </summary>
        protected string EndpointKey { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AbstractDocumentDbOperation"/> class.
        /// </summary>
        /// <param name="endpointUrlName">Appsetting key for Url to DocumentDb endpoint.</param>
        /// <param name="endpointKeyName">Appsetting key for Key for DocumentDB</param>
        /// <param name="documentDatabase">The Database to query</param>
        /// <param name="documentCollection">The DocumentCollection to query</param>
        protected AbstractDocumentDbOperation(string endpointUrlName, string endpointKeyName, string documentDatabase, string documentCollection)
        {
            Guard.Against<ArgumentException>(string.IsNullOrEmpty(endpointUrlName),
                                             "endpointUrlName string name must have a value");

            Guard.Against<ArgumentException>(string.IsNullOrEmpty(endpointKeyName),
                                             "endpointKeyName string name must have a value");

            Guard.Against<ArgumentException>(string.IsNullOrEmpty(documentDatabase),
                                             "documentDatabase string name must have a value");

            Guard.Against<ArgumentException>(string.IsNullOrEmpty(documentCollection),
                                             "documentCollection string name must have a value");

            EndpointUrl = ConfigurationManager.AppSettings[endpointUrlName]; 
            EndpointKey = ConfigurationManager.AppSettings[endpointKeyName];
            DocumentDatabase = documentDatabase;
            DocumentCollection = documentCollection;
            UseTransaction = false;

            Client = new DocumentClient(new Uri(EndpointUrl),
                   EndpointKey,
                   new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp });
        }
    }
}
