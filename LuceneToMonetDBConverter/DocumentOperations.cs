using Lucene.Net.Documents;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LuceneToMonetDBConverter
{
    public static class DocumentOperations
    {
        public static string TryGetValue(this Document doc, string name)
        {
            var values = doc.GetValues(name);
            return values.Any() ? values[0] : null;
        }

        public static string[] TryGetValues(this Document doc, string name)
        {
            var values = doc.GetValues(name);
            return values.Any() ? values : null;
        }
    }
}
