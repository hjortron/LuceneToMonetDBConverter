using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LuceneToMonetDBConverter
{
    class Program
    {
        readonly static DateTime StartDate = new DateTime(1970, 1, 1);
        readonly static DateTime EndDate = new DateTime(2038, 1, 19);

        static void Main(string[] args)
        {
            var folders = Directory.GetDirectories(Settings.Default.CustomersDirectory)
                         .Where(folder => folder.Contains("RawData")).ToList();
            foreach (var path in folders)
            {
                Console.WriteLine("Converting " + path);               
                var reader = new LuceneStorageReader(path);
                var luceneDocuments = reader.GetByDateRange(StartDate, EndDate).ToList();
                MonetDbWriter.InsertDocumnetsIntoTable(luceneDocuments);
                Console.Write("Done");
            }
            Console.ReadKey();
        }


    }
}
