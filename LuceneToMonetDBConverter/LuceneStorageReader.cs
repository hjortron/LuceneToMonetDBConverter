using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using BoboBrowse.Api;
using BoboBrowse.Facets;
using BoboBrowse.Facets.data;
using BoboBrowse.Facets.impl;

using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;

namespace LuceneToMonetDBConverter
{
    public sealed class LuceneStorageReader : IDisposable
    {
        private enum FieldType
        {
            None,
            String,
            Int,
            Float,
            MultiString,
        }

        public enum DataType
        {
            Search,
            PageTrackingEvents,
            Reindex,
            AllEvents
        }
        
        private static readonly System.Diagnostics.Stopwatch Stopwatch = new System.Diagnostics.Stopwatch();      

        private readonly string _indexDir;

        private static class Consts
        {
            public static readonly Dictionary<string, FieldType> FieldNamesTypes = new Dictionary<string, FieldType>
            {               
                {"TimeStamp", FieldType.Int}, 
                {"Hash", FieldType.String}, 
                {"UserIP", FieldType.String}, 
                {"UserAgent", FieldType.String}, 
                {"UserID", FieldType.String}, 
                {"SessionID", FieldType.String}, 
                {"EventType", FieldType.String}, 

                {"UserSearchPhrase", FieldType.String}, 
                {"CorrectedSearchPhrase", FieldType.String}, 
                {"Prefilter", FieldType.String}, 
                {"HiddenString", FieldType.String}, 
                {"SearchResultState", FieldType.String}, 
                {"GroupField", FieldType.String}, 
                {"SortType", FieldType.Int}, 
                {"ViewType", FieldType.Int}, 
                {"PageNumber", FieldType.Int}, 
                {"ResultsCount", FieldType.Int}, 

                {"ColorValue0", FieldType.String}, 
                {"ColorValue1", FieldType.String}, 
                {"ColorValue2", FieldType.String}, 
                {"ColorValue3", FieldType.String}, 
                {"ColorValue4", FieldType.String}, 
                {"ColorWeight0", FieldType.Float}, 
                {"ColorWeight1", FieldType.Float}, 
                {"ColorWeight2", FieldType.Float}, 
                {"ColorWeight3", FieldType.Float}, 
                {"ColorWeight4", FieldType.Float}, 

                {"Facet", FieldType.MultiString}, 
                {"FacetName", FieldType.String},
                {"FacetValue", FieldType.String},
                {"Trigger", FieldType.MultiString}, 

                {"ElapsedTime", FieldType.Int}, 
                {"SearchFeatures", FieldType.String}, 
                {"ShopUserID", FieldType.String}, 
                {"ShopUserName", FieldType.String}, 
                  
                {"ProductId", FieldType.String}, 
                {"ProductUrl", FieldType.String},                 
                {"Position", FieldType.Int}, 
                {"Total", FieldType.Int},                 
            };
        }

        private BoboIndexReader _reader;

        public void ReopenReader()
        {
            if (_reader != null)
            {
                _reader.Close(); // if throws add try catch
            }

            _reader = OpenBoboReader(_indexDir);
        }

        public LuceneStorageReader(string pageTrackingDir)
        {
            _indexDir = pageTrackingDir;
            _reader = OpenBoboReader(_indexDir);
        }

        private IndexReader OpenReader(string directory)
        {
            return IndexReader.Open(new SimpleFSDirectory(new DirectoryInfo(directory)), true);
        }

        private BoboIndexReader OpenBoboReader(string indexDir)
        {
            return BoboIndexReader.GetInstance(OpenReader(indexDir), CreateFacetHandlers());
        }

        private ICollection<FacetHandler> CreateFacetHandlers()
        {
            var facetHandlers = new List<FacetHandler>();
            foreach (var field in Consts.FieldNamesTypes)
            {
                switch (field.Value)
                {
                    case FieldType.None:
                        facetHandlers.Add(new SimpleFacetHandler(field.Key));
                        break;
                    case FieldType.String:
                        facetHandlers.Add(new SimpleFacetHandler(field.Key, new PredefinedTermListFactory<string>()));
                        break;
                    case FieldType.MultiString:
                        facetHandlers.Add(new MultiValueFacetHandler(field.Key, new PredefinedTermListFactory<string>()));
                        break;
                    case FieldType.Int:
                         facetHandlers.Add(new SimpleFacetHandler(field.Key, new PredefinedTermListFactory<int>()));
                        break;
                    case FieldType.Float:
                        facetHandlers.Add(new SimpleFacetHandler(field.Key, new PredefinedTermListFactory<float>()));
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            return facetHandlers;
        }

        public Document GetDocument(int docId)
        {
            var doc =  _reader.Document(docId);
            return doc;
        }

        public Dictionary<string, object> GetDocFields(int docId)
        {
            var doc = GetDocument(docId);
            var values = GetDocFields(doc);
            values.Add("_docid", docId);
            return values;
        }

        private Dictionary<string, object> GetDocFields(Document doc)
        {
            var fieldsValues = new Dictionary<string, object>();

            foreach (var field in Consts.FieldNamesTypes)
            {
                var fieldName = field.Key;
                var fieldables = doc.GetFieldables(fieldName);
                if (fieldables == null || !fieldables.Any())
                {
                    continue;
                }

                foreach (var fld in fieldables)
                {
                    if (fieldsValues.ContainsKey(fieldName))
                    {
                        fieldsValues[fieldName] += "; " + fld.StringValue();
                    }
                    else
                    {
                        fieldsValues.Add(fieldName, fld.StringValue());
                    }
                }
            }

            return fieldsValues;

        }

        public void Dispose()
        {
            if (_reader != null)
            {
                _reader.Dispose();
            }
        }

        private IEnumerable<TrackEventType> GetAllUserEventsNames()
        {
            return Enum.GetValues(typeof(TrackEventType)).Cast<TrackEventType>().Where(e => e != TrackEventType.Reindex);
        }

        private IEnumerable<TrackEventType> GetPageTrackingEventsNames()
        {
            return Enum.GetValues(typeof(TrackEventType)).Cast<TrackEventType>().Where(e => e != TrackEventType.Reindex && e != TrackEventType.Search);
        }

        public IEnumerable<Document> GetByDateRange(DateTime startDate, DateTime endDate, IEnumerable<string> hashes = null, DataType dataType = DataType.AllEvents)
        {
            const int onceBrowseCount = 10000;

            var query = new BooleanQuery();

            query.Add(new TermRangeQuery("TimeStamp",
                NumericUtils.IntToPrefixCoded(startDate.ToUnixTime()),
                NumericUtils.IntToPrefixCoded(endDate.ToUnixTime()),
                true, true), BooleanClause.Occur.MUST);

            var browseRequest = new BrowseRequest {Query = query};

            if (hashes !=null)
            { 
                var hashSelection = new BrowseSelection("Hash");
                foreach (var hash in hashes)
                {
                    hashSelection.AddValue(hash);
                }
                browseRequest.AddSelection(hashSelection);
            }

            var selection = new BrowseSelection("EventType");
            switch (dataType)
            {
                case DataType.Search:
                    {
                        selection.AddValue(TrackEventType.Search.ToString());
                        break;
                    }
                case DataType.Reindex:
                    {
                        selection.AddValue(TrackEventType.Reindex.ToString());
                        break;
                    }
                case DataType.PageTrackingEvents:
                    {
                        foreach (var eventType in GetPageTrackingEventsNames())
                        {
                            selection.AddValue(eventType.ToString());
                        }
                        break;
                    }
                default:
                    {
                        foreach (var eventType in GetAllUserEventsNames())
                        {
                            selection.AddValue(eventType.ToString());
                        }
                        break;
                    }
            }
            browseRequest.AddSelection(selection);

            browseRequest.FacetSpecs = new Dictionary<string, FacetSpec>
            {
                {"SessionID", new FacetSpec()},
                {"Hash", new FacetSpec()},
            };

            ReopenReader();
            var browser = new BoboBrowser(_reader);
            var emptyResult = browser.Browse(browseRequest);

            browseRequest.Count = onceBrowseCount;

            var list = new List<Document>();
            for (int i = 0; i < emptyResult.NumHits; i += onceBrowseCount)
            {
                browseRequest.Offset = i;
                var partResult = browser.Browse(browseRequest);

                list.AddRange(partResult.Hits.Select(h => GetDocument(h.DocId)));
            }

             return list;
        }

        public IEnumerable<Document> GetBySessionId(string sessionId, DataType dataType = DataType.AllEvents)
        {
            const int onceBrowseCount = 10000;

            var query = new BooleanQuery();

            query.Add(new TermQuery(new Term("SessionID", sessionId)), BooleanClause.Occur.MUST);

            var browseRequest = new BrowseRequest();                    

            var selection = new BrowseSelection("EventType");
            switch (dataType)
            {
                case DataType.Search:
                    {
                        selection.AddValue(TrackEventType.Search.ToString());
                        break;
                    }
                case DataType.Reindex:
                    {
                        selection.AddValue(TrackEventType.Reindex.ToString());
                        break;
                    }
                case DataType.PageTrackingEvents:
                    {
                        foreach (var eventType in GetPageTrackingEventsNames())
                        {
                            selection.AddValue(eventType.ToString());
                        }
                        break;
                    }
                default:
                    {
                        foreach (var eventType in GetAllUserEventsNames())
                        {
                            selection.AddValue(eventType.ToString());
                        }
                        break;
                    }
            }

            browseRequest.AddSelection(selection);

            browseRequest.Query = query;

            browseRequest.FacetSpecs = new Dictionary<string, FacetSpec>
            {
                {"SessionID", new FacetSpec()},
                {"Hash", new FacetSpec()},
            };
           
            var browser = new BoboBrowser(_reader);
            var emptyResult = browser.Browse(browseRequest);

            browseRequest.Count = onceBrowseCount;

            var list = new List<Document>();
            for (int i = 0; i < emptyResult.NumHits; i += onceBrowseCount)
            {
                browseRequest.Offset = i;
                var partResult = browser.Browse(browseRequest);

                list.AddRange(partResult.Hits.Select(h => GetDocument(h.DocId)));
            }
            return list;
        }    
    }

    public static class DateTimeExt
    {
        public static readonly DateTime UnixBase = new DateTime(1970, 1, 1, 0, 0, 0, 0);

        public static DateTime UnixMaxDateTime
        {
            get { return UnixBase.AddSeconds(int.MaxValue); }
        }

        public static int ToUnixTime(this DateTime dateTime)
        {
            return (int)(dateTime - UnixBase).TotalSeconds;
        }

        public static DateTime ToDateTime(this int unixTime)
        {
            return UnixBase.AddSeconds(unixTime).ToLocalTime();
        }
    }

    public enum TrackEventType
    {
        None = 0,
        OpenPage,
        ClosePage,
        UserActive,
        UserIdle,
        ClickOnSearchResult,
        ProductView,
        AddToCart,
        ConfirmOrder,
        Search,
        SuggestionSelection,
        Reindex
    }  
}