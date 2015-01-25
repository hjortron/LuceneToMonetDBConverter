using Lucene.Net.Documents;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace LuceneToMonetDBConverter
{
    static class  MonetDbWriter
    {
        private static string[] _intColumnsHeaders =
        {
            "resultscount",
            "elapsedtime",
            "Position",
            "total",
            "colorweight0",
            "colorweight1",
            "colorweight2",
            "colorweight3",
            "colorweight4",
            "sorttype",
            "viewtype",
            "pagenumber"             
        };  
        public static void InsertDocumnetsIntoTable(IEnumerable<Document> documents)
        {
            var monetDbC = new OdbcConnection(Settings.Default.MonetDbConnectionString);
            var docs = documents as Document[] ?? documents.ToArray();
            var documentsCount = docs.Count();
            var percent = documentsCount*.01;
            Console.WriteLine("Copy in progress...");
            var percentageOutput = "0%";
            Console.Write("Ready: " + percentageOutput);
            var odbcCmd = new OdbcCommand { Connection = monetDbC };
            monetDbC.Open();
            var count = 0;
            var per = 0;
          
            foreach (var document in docs)
            {
                try
                {
                    odbcCmd.CommandText = GetQueryForDocument(document);
                    odbcCmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {                    
                    throw ex;
                }
                ++count;
                if (!(count > 5*percent)) continue;
                ++per;
                count = 0;
                Console.SetCursorPosition(Console.CursorLeft - percentageOutput.Length, Console.CursorTop);
                percentageOutput = per*5 + "%";
                Console.Write(percentageOutput);
            }
            Console.SetCursorPosition(Console.CursorLeft - percentageOutput.Length, Console.CursorTop);
            Console.WriteLine("100%");
            monetDbC.Close();
        }

        private static string GetQueryForDocument(Document document)
        {
            var fieldNamesList = new List<string>();
            var fieldValuesList = new List<string>();
            var facetItr = 0;
            foreach (Field field in  document.GetFields())
            {
                var fieldName = field.Name();

                var fieldValue = field.StringValue();
                if (fieldValue == "")
                {
                    continue;
                }
                              
                switch (fieldName)
                {
                    case "TimeStamp":
                        fieldName = "datetime";
                        fieldValue = (int.Parse(fieldValue)).ToDateTime().ToString("yyyy-MM-dd HH:mm:ss");
                        break;
                    case "Facet":
                        var facet = JsonConvert.DeserializeObject<KeyValuePair<string, string>>(fieldValue);
                        fieldNamesList.AddRange(new[] {"FacetName" + facetItr, "FacetValue" + facetItr++});
                        fieldValuesList.Add(String.Format("'{0}'", ReplaceEscapeStrings(facet.Key)));
                        fieldValuesList.Add(String.Format("'{0}'", ReplaceEscapeStrings(facet.Value)));
                        continue;
                }

                if (!_intColumnsHeaders.Contains(fieldName.ToLower()))
                {
                    fieldValue = String.Format("'{0}'", ReplaceEscapeStrings(fieldValue));
                }

                fieldNamesList.Add(fieldName);
                fieldValuesList.Add(fieldValue);
            }

            var fieldNames = string.Join(",", fieldNamesList);
            var fieldValues = string.Join(",", fieldValuesList);

            return String.Format("INSERT INTO {0}({1}) VALUES({2})","trackedevents", fieldNames, fieldValues);
        }

        private static string ReplaceEscapeStrings(string field)
        {
            return field.Replace(@"\'", "'").Replace("'", "''");
        }
    }
}
