using Rhino.Etl.Core;
using Rhino.Etl.Dsl;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleTest
{
    class Program
    {
        static void Main(string[] args)
        {
            using (EtlProcess process = CreateDslInstance("Dsl/UsersToPeopleBulk.boo"))
                process.Execute();
        }

        protected static EtlProcess CreateDslInstance(string url)
        {
            return EtlDslEngine.Factory.Create<EtlProcess>(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, url));
        }
    }
}
