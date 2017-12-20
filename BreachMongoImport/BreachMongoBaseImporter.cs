using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BreachMongoImport
{
    public abstract class BreachMongoBaseImporter
    {
        private string _bufferedEmail;
        private List<string> _bufferedPass = new List<string>();

        private string _dataPath;
        private string _dbName;
        private string _collection;

        public BreachMongoBaseImporter(string dataPath, string dbName, string collection)
        {
            _dataPath = dataPath;
            _dbName = dbName;
            _collection = collection;
        }

        public int MaxPassCount { get; private set; }

        public IEnumerable<string> GetProcessItems(string dataPath)
        {
            return Directory.GetDirectories(dataPath)
                .Union(Directory.GetFiles(dataPath));
        }

        public long ConvertFile(string processItem, string outFile)
        {
            var files = (Directory.Exists(processItem))
                ? GetDirOrderedFiles(processItem).ToList()
                : new List<string>(new [] { processItem });

            return WriteConvertedFile(files, outFile);
        }

        public void ConvertAll(string outPath, string importFile)
        {
            var processItems = GetProcessItems(_dataPath);

            foreach (var item in processItems)
            {
                var outFile = outPath + Path.GetFileName(item);

                Console.WriteLine("[{0}] Processing '{1}'...",
                    DateTime.Now, Path.GetFileName(item));

                var count = ConvertFile(item, outFile);

                var command = GeMongoImportCommand(_dbName, _collection, outFile);
                var importParams = GetMongoImportParams();

                AddImportCommand(importFile, command + importParams);

                Console.WriteLine("[{0}] Processed '{1}' pair.", DateTime.Now, count);
            }

            Console.Write("Done!");
        }

        protected abstract string GetFormatedString(string email, IEnumerable<string> pass);

        protected abstract string GetMongoImportParams();

        private long WriteConvertedFile(IEnumerable<string> sourceFiles, string destFile)
        {
            long pairCount = 0;

            using (var writer = File.CreateText(destFile))
            {
                foreach (var sourceFile in sourceFiles)
                {
                    foreach (var line in File.ReadLines(sourceFile, Encoding.UTF8))
                    {
                        var strs = line.Split(
                            new[] { ' ', '\t', ',', ';', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);

                        foreach (var str in strs)
                        {
                            // parse values
                            var splitIndex = str.IndexOf(':');

                            if ((splitIndex > 0) || str.Contains('@'))
                            {
                                var email = (splitIndex > 0) ? str.Substring(0, splitIndex) : str;

                                var pass = (splitIndex > 0)
                                    ? str.Substring(splitIndex + 1, str.Length - splitIndex - 1)
                                    : string.Empty;

                                var passCount = AddEmailPassPair(writer, email.ToLower(), pass);

                                if (passCount > MaxPassCount)
                                {
                                    MaxPassCount = passCount;
                                }

                                ++pairCount;
                            }
                        }
                    }
                }

                // flush buffered values
                AddEmailPassPair(writer, null, null);

                return pairCount;
            }
        }

        private int AddEmailPassPair(StreamWriter writer, string email, string pass)
        {
            if (_bufferedEmail != email)
            {
                if (!string.IsNullOrEmpty(_bufferedEmail))
                {
                    var str = GetFormatedString(_bufferedEmail, _bufferedPass);
                    writer.WriteLine(str);
                }

                _bufferedPass.Clear();
            }

            _bufferedEmail = email;

            if (!string.IsNullOrEmpty(pass) && !_bufferedPass.Contains(pass))
            {
                _bufferedPass.Add(pass);
            }

            return _bufferedPass.Count;
        }

        private static string GeMongoImportCommand(string dbName, string collection, string importFile)
        {
            return $"mongoimport -d {dbName} -c {collection} --file {importFile} --mode merge --numInsertionWorkers 4 ";
        }

        private static void AddImportCommand(string importFile, string command)
        {
            using (var importWriter = File.AppendText(importFile))
            {
                importWriter.WriteLine(command);
            }
        }

        private static IEnumerable<string> GetDirOrderedFiles(string dir)
        {
            var files = Directory.GetFiles(dir)
                .OrderBy(f => Path.GetFileNameWithoutExtension(f));

            var dirs = Directory.GetDirectories(dir)
                .OrderBy(d => d);

            var items = files.Union(dirs).OrderBy(k => k).ToArray();

            foreach (var item in items)
            {
                if (files.Contains(item))
                {
                    yield return item;
                }
                else
                {
                    var subDirFiles = GetDirOrderedFiles(item);

                    foreach (var file in subDirFiles)
                    {
                        yield return file;
                    }
                }
            }
        }

    }
}
