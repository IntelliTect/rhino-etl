namespace Rhino.Etl.Core.DataReaders
{
    using System;
    using System.Collections.Generic;
    using System.Dynamic;
    using System.Linq;

    /// <summary>
    /// A datareader over a collection of dictionaries
    /// </summary>
    public class DocumentDbDataReader : EnumerableDataReader
    {
        private readonly IEnumerable<dynamic> enumerable;
        private readonly List<Descriptor> propertyDescriptors = new List<Descriptor>();

        /// <summary>
        /// Initializes a new instance of the <see cref="DictionaryEnumeratorDataReader"/> class.
        /// </summary>
        /// <param name="enumerable">The enumerator.</param>
        public DocumentDbDataReader(
            IEnumerable<dynamic> enumerable)
            : base(enumerable.GetEnumerator())
        {
            this.enumerable = enumerable;
            DynamicObject d = enumerable.FirstOrDefault() as DynamicObject;
           
            foreach (var name in d.GetDynamicMemberNames())
            {
                propertyDescriptors.Add(new DynamicDescriptorAdapter(new KeyValuePair<string, Type>(name, typeof(string))));
            }
        }

        /// <summary>
        /// Gets the descriptors for the properties that this instance
        /// is going to handle
        /// </summary>
        /// <value>The property descriptors.</value>
        protected override IList<Descriptor> PropertyDescriptors
        {
            get { return propertyDescriptors; }
        }

        /// <summary>
        /// Perform the actual closing of the reader
        /// </summary>
        protected override void DoClose()
        {
            IDisposable disposable = enumerator as IDisposable;
            if (disposable != null)
                disposable.Dispose();

            disposable = enumerable as IDisposable;
            if (disposable != null)
                disposable.Dispose();
        }
    }
}