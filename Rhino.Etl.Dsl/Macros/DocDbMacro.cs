using Rhino.Etl.Core.ConventionOperations;
using Rhino.Etl.Core.Operations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rhino.Etl.Dsl.Macros
{
    /// <summary>
    /// Create a new <see cref="InputCommandOperation"/> and instantiate it in place of the
    /// macro
    /// </summary>
    public class DocDbInputMacro : AbstractClassGeneratorMacro<DocDbInputCommandOperation>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="InputMacro"/> class.
        /// </summary>
        public DocDbInputMacro() : base(null)
        {
        }
    }
}
