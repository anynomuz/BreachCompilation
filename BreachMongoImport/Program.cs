using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace BreachMongoImport
{
    public class Program
    {
        static void Main(string[] args)
        {
            var dataPath = "d:\\breachCompilation\\data\\";
            var dbname = "leaks";
            var collection = "emails";

            var importer = new BreachMongoCsvImporter(dataPath, dbname, collection);

            var outPath = Directory.GetCurrentDirectory() + "\\csv\\";
            Directory.CreateDirectory(outPath);
            var importFile = outPath + "import.cmd";

            importer.ConvertAll(outPath, importFile);

            Console.ReadKey();
        }
    }
}
