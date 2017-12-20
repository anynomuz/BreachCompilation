using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace BreachMongoImport
{
    public class BreachMongoJsonImporter
        : BreachMongoBaseImporter
    {
        public BreachMongoJsonImporter(string dataPath, string dbName, string collection)
            : base(dataPath, dbName, collection)
        {
        }

        protected override string GetFormatedString(string email, IEnumerable<string> pass)
        {
            var passStr = (pass.Any())
                ? string.Join(",", pass.Select(p => JsonConvert.ToString(p)))
                : "\"\"";

            email = JsonConvert.ToString(email);

            return "{ \"_id\": " + email + ", \"pass\": [" + passStr + "] }";
        }


        protected override string GetMongoImportParams()
        {
            return "--type json";
        }
    }
}
