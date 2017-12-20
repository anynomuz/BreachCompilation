using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace BreachMongoImport
{
    public class BreachMongoCsvImporter
        : BreachMongoBaseImporter
    {
        public BreachMongoCsvImporter(string dataPath, string dbName, string collection)
            : base(dataPath, dbName, collection)
        {
        }

        protected override string GetFormatedString(string email, IEnumerable<string> pass)
        {
            var passStr = (pass.Any())
                ? string.Join(",", pass.Select(p => "\"" + p.Replace("\"", "\"\"") + "\""))
                : "\"\"";

            return "\"" + email.Replace("\"", "\"\"") + "\"," + passStr;
        }

        protected override string GetMongoImportParams()
        {
            var list = new List<string>();
            list.Add("pass");

            // too long list
            ////for (var i = 1; i < MaxPassCount; ++i)
            ////{
            ////    list.Add("pass" + i);
            ////}

            var fields = "_id, " + string.Join(", ", list);

            return $"--type csv --fields \"{fields}\"";
        }
    }
}
