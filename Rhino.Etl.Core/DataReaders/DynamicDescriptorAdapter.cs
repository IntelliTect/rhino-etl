namespace Rhino.Etl.Core.DataReaders
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using Microsoft.CSharp;
    using Dynamitey;

    /// <summary>
    /// Adapts a dynamic to a descriptor
    /// </summary>
    public class DynamicDescriptorAdapter : Descriptor
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicDescriptorAdapter"/> class.
        /// </summary>
        /// <param name="pair">The pair.</param>
        public DynamicDescriptorAdapter(KeyValuePair<string, Type> pair) : base(pair.Key, pair.Value)
        {
        }

        /// <summary>
        /// Gets the value.
        /// </summary>
        /// <param name="obj">The obj.</param>
        /// <returns></returns>
        public override dynamic GetValue(object obj)
        {
            return Dynamic.InvokeGet(obj, Name);
        }
    }
}