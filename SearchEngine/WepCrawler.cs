using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.IO;
using mshtml;
using System.Data.SqlClient;
using System.Data;
using System.Threading;
using System.Linq;
using HtmlAgilityPack;
using System.Text.RegularExpressions;
namespace ConsoleApp1
{
    class WepCrawler
    {
        //first module
        static int storedSize = 1000;
        static int retrivedDOCS = 1500;
        static string connStr = "Data Source=DESKTOP-KMUSS9C;Initial Catalog=wepCrawling;Integrated Security=True";
        static SqlConnection connection;
        static Dictionary<string, string> url_Doc_dict1 = new Dictionary<string, string>();
        static Dictionary<string, string> url_Doc_dict2 = new Dictionary<string, string>();
        static Dictionary<string, string> url_Doc_dict3 = new Dictionary<string, string>();
        //second module
        static string[] STOP_WORD_LIST = new string[]{
            "a","about","above","again","against","all","am","an","and","any","are","aren't","as","at","be","because",
            "been","before","being","below","between","both","but","by","can't","cannot","could","couldn't","did",
            "didn't","do","does","doesn't","doing","don't","down","during","each","few","for","from",
            "further","had","hadn't","has","hasn't","have","haven't","having","he","he'd",
            "he'll","he's","her","here","here's","hers","herself","him","himself","his","how","how's",
            "i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself","let's","me",
            "more","most","mustn't","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours",
            "ourselves", "out", "overown", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
            "than", "that", "that's", "the", "their", "theirs", "them themselves", "then", "there", "there's", "these", "they",
            "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't",
            "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what","what's","when","when's","where","where's","which","while",
            "who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves"
                    };
        static Dictionary<string, string> invertedIndex = new Dictionary<string, string>();
        static Dictionary<int, StringBuilder> DOCS = new Dictionary<int, StringBuilder>();

        //third module
        public static List<string> Search(string query)
        {
            List<int> ids = getIds(query);
            List<string> links = new List<string>();
            connection = new SqlConnection(connStr);
            connection.Open();
            string q;
            foreach (int id in ids)
            {
                q = "SELECT [lnk] FROM [wepCrawling].[dbo].[docTable] where id=" + id+"";
                SqlCommand cmd = new SqlCommand(q, connection);
                cmd.CommandType = CommandType.Text;
                SqlDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    links.Add(reader["lnk"].ToString());
                }
                reader.Close();
            }
            connection.Close();
            return links;
        }

        private static List<int> getIds(string query)
        {
            List<string> queryToken = getQueryTokens(query);
            List<List<KeyValuePair<int, string>>> docid_pos_list = new List<List<KeyValuePair<int, string>>>();
            foreach (var item in queryToken)
            {
                docid_pos_list.Add(getindex(item));
            }
            Dictionary<int, List<string>> doc_poss_intersect = getIntersection(docid_pos_list);
            Dictionary<int, int> selectedDoc;
            if (query.StartsWith("\""))
                selectedDoc = getWordSeqList(doc_poss_intersect);
            else 
                selectedDoc = getTotalDist(doc_poss_intersect);
            List<int> doc_ids = new List<int>();
            foreach (KeyValuePair<int, int> item in selectedDoc.OrderBy(key => key.Value))
            {
                Console.WriteLine(item.Key + " val :: " + item.Value);
                doc_ids.Add(item.Key);
            }
            return doc_ids;
        }

        private static Dictionary<int, int> getTotalDist(Dictionary<int, List<string>> doc_poss_intersect)
        {
            Dictionary<int, int> TotalDist = new Dictionary<int, int>();
            foreach (var item in doc_poss_intersect) //by doc
            {
                int docid = item.Key;
                List<List<int>> pos = new List<List<int>>();
                foreach (var poss in item.Value) //by word
                {
                    var listOfInts = poss.Split(',').Select(Int32.Parse).ToList();
                    pos.Add(listOfInts);
                }
                //  1,4,13    5,2,9,6    22,11,4
                //        min1       min2
                /////////totaldist1/////////       
                TotalDist.Add(docid, getMinDistance(pos));
            }
            return TotalDist;
        }

        //search for "query"
        private static Dictionary<int, int> getWordSeqList(Dictionary<int, List<string>> doc_poss_intersect)
        {
            Dictionary<int, int> docId_freq = new Dictionary<int, int>();
            foreach (var item in doc_poss_intersect)
            {
                int docid = item.Key;
                List<List<int>> pos = new List<List<int>>();
                foreach (var poss in item.Value)
                {
                    var listOfInts = poss.Split(',').Select(Int32.Parse).ToList();
                    pos.Add(listOfInts);
                } 
                /// 1,4,13       w1 poss
                /// 5,2,9,6      w2 poss
                /// 22,11,4,6    w3 poss
                int checkSeqCount = 0;
                int freqCount = 0;
                for (int i = 0; i < pos.ElementAt(0).Count; i++) //iter first list
                {
                    int fWordPos = pos.ElementAt(0).ElementAt(i); 
                    for (int j = 1; j < pos.Count; j++)//iter all list except first
                    {
                        for (int k = 0; k < pos.ElementAt(j).Count; k++) //iter next list
                        {
                            int secWordPos = pos.ElementAt(j).ElementAt(k);
                            if (secWordPos - fWordPos == 1)
                            {
                                checkSeqCount++;
                                fWordPos = secWordPos;
                                break;
                            }
                        }
                        if (checkSeqCount == 0) break;
                    }
                    if (checkSeqCount == pos.Count - 1)
                        freqCount++;
                    checkSeqCount = 0;
                }
                freqCount -= 1;
                if (freqCount>0)
                    docId_freq.Add(docid,freqCount*-1);
            }
            return docId_freq;
        }

        private static int getMinDistance(List<List<int>> pos)
        {
            int totalMin = 0;
            for (int i = 1; i < pos.Count; i++)
            {
                List<int> a = pos.ElementAt(i - 1);
                List<int> b = pos.ElementAt(i);
                List<int> minList = new List<int>();
                for (int k = 0; k < a.Count; k++)
                {
                    int min = 9999999;
                    for (int j = 0; j < b.Count; j++)
                    {
                        int absDiff = Math.Abs(a.ElementAt(k) - b.ElementAt(j));
                        if (absDiff < min)
                        {
                            min = absDiff;
                        }
                    }
                    minList.Add(min);
                }
                totalMin += minList.Min();
            }
            if (totalMin == 0) return int.MaxValue;
            return totalMin;

        }
        private static Dictionary<int, List<string>> getIntersection(List<List<KeyValuePair<int, string>>> doc_pos_list)
        {
            Dictionary<int, List<string>> intersectionDict = new Dictionary<int, List<string>>();
            List<KeyValuePair<int, string>> min_doc_poss = new List<KeyValuePair<int, string>>();
            int min = 999999999;
            int index = 0;
            for (int i = 0; i < doc_pos_list.Count; i++)
            {
                if (doc_pos_list.ElementAt(i).Count < min)
                {
                    min = doc_pos_list.ElementAt(i).Count;
                    index = i;
                }
            }
            min_doc_poss = doc_pos_list.ElementAt(index);
            doc_pos_list.RemoveAt(index);


            for (int i = 0; i < min_doc_poss.Count; i++)
            {
                List<string> posList = new List<string>();
                posList.Add(min_doc_poss.ElementAt(i).Value);
                for (int j = 0; j < doc_pos_list.Count; j++)
                {
                    for (int k = 0; k < doc_pos_list.ElementAt(j).Count; k++)
                    {
                        KeyValuePair<int, string> item = doc_pos_list.ElementAt(j).ElementAt(k);
                        if (item.Key == min_doc_poss.ElementAt(i).Key)
                        {
                            posList.Add(item.Value);
                        }
                    }
                }
                if (posList.Count > 0)
                {
                    intersectionDict.Add(min_doc_poss.ElementAt(i).Key, posList);
                }
            }
            return intersectionDict;
        }
        private static List<KeyValuePair<int, string>> getindex(string value)
        {
            List<KeyValuePair<int, string>> indexs = new List<KeyValuePair<int, string>>();
            string q = "SELECT [pos],[doc_id] FROM [wepCrawling].[dbo].[invertedIndex] where [term] ='" + value + "' order by [freq] desc";
            connection = new SqlConnection(connStr);
            connection.Open();
            SqlCommand cmd = new SqlCommand(q, connection);
            cmd.CommandType = CommandType.Text;

            SqlDataReader reader = cmd.ExecuteReader();
            string poss;
            int id = 0;
            while (reader.Read())
            {
                poss = reader["pos"].ToString();
                id = Convert.ToInt32(reader["doc_id"].ToString());
                indexs.Add(new KeyValuePair<int, string>(id, poss));
            }
            reader.Close();
            connection.Close();
            return indexs;
        }
        private static List<string> getQueryTokens(string query)
        {
            string[] words = getTokens(new StringBuilder(query));
            words = caseFolding(words);
            List<string> newTokens = stopWordRemovel(words);
            List<string> tokenAfterSteaming = steaming(newTokens);
            return tokenAfterSteaming;
        }


        //second module
        public static void initIndexDB()
        {
            Console.WriteLine("fetching.....");
            fetchDOCS();
            Console.Clear();
            Console.WriteLine("indexing.....");
            Indexing();
            Console.Clear();
            Console.WriteLine("inserting.....");
            foreach (var index in invertedIndex)
            {
                //key ceaser:1 value 1,4,2,3
                string[] key = index.Key.Split(":");
                string[] value = index.Value.Split(",");
                string term = key[0];
                int docId = Convert.ToInt32(key[1]);
                string positions = index.Value;
                int freq = value.Length;
                //Console.WriteLine("term--> "+term+ "\t\tfreq-->" + freq + "\tdoc id-->" + docId + "\tpositions-->" + positions);
                INSERT_INDEXS(term, freq, docId, positions);
            }
            Console.Clear();
        }
        private static void INSERT_INDEXS(string term, int freq, int docId, string positions)
        {
            connection = new SqlConnection(connStr);
            connection.Open();
            string query = "INSERT INTO dbo.invertedIndex VALUES (@term,@freq,@positions,@doc)";
            SqlCommand cmdInsert = new SqlCommand(query, connection);
            cmdInsert.Parameters.AddWithValue("@term", term);
            cmdInsert.Parameters.AddWithValue("@freq", freq);
            cmdInsert.Parameters.AddWithValue("@positions", positions);
            cmdInsert.Parameters.AddWithValue("@doc", docId);
            cmdInsert.ExecuteNonQuery();
            connection.Close();
        }
        private static void Indexing()
        {
            foreach (var item in DOCS)
            {
                int id = item.Key;
                StringBuilder sb = item.Value;
                string[] tokens = getTokens(sb);
                tokens = caseFolding(tokens);
                //to generate inverted index 
                List<KeyValuePair<string, int>> tokenDict = generatetokenWithPos(tokens);
                List<string> newTokens = stopWordRemovel(tokens);
                List<string> tokenAfterSteaming = steaming(newTokens);
                generateInvertedIndex(id, tokenAfterSteaming, tokenDict);//key ceaser:1 value 1,4,2,3

            }
        }
        private static void fetchDOCS()
        {
            string q = "SELECT TOP " + retrivedDOCS + " [id],[doc] FROM [wepCrawling].[dbo].[docTable]";
            connection = new SqlConnection(connStr);
            connection.Open();
            SqlCommand cmd = new SqlCommand(q, connection);
            cmd.CommandType = CommandType.Text;

            SqlDataReader reader = cmd.ExecuteReader();
            StringBuilder sb = new StringBuilder();
            int i = 0;

            while (reader.Read())
            {
                sb = new StringBuilder(reader["doc"].ToString());
                i = Convert.ToInt32(reader["id"].ToString());
                DOCS.Add(i, sb);
            }
            reader.Close();
            connection.Close();
        }
        private static void generateInvertedIndex(int DocId, List<string> tokenAfterSteaming, List<KeyValuePair<string, int>> tokenDict)
        {
            for (int i = 0; i < tokenAfterSteaming.Count; i++)
            {
                string key = tokenAfterSteaming.ElementAt(i) + ":" + DocId;
                if (!invertedIndex.ContainsKey(key))
                {
                    List<int> values = (from kvp in tokenDict where kvp.Key == tokenAfterSteaming.ElementAt(i) select kvp.Value).ToList();
                    invertedIndex.Add(key, string.Join(",", values.ToArray()));
                }
            }
        }
        private static List<KeyValuePair<string, int>> generatetokenWithPos(string[] tokens)
        {
            PorterSteamer steamer = new PorterSteamer();
            List<KeyValuePair<string, int>> tokenDict = new List<KeyValuePair<string, int>>();
            for (int i = 0; i < tokens.Length; i++)
            {
                tokenDict.Add(new KeyValuePair<string, int>(steamer.StemWord(tokens[i]), i));
            }
            return tokenDict;
        }
        private static List<string> steaming(List<string> newTokens)
        {
            PorterSteamer steamer = new PorterSteamer();
            List<string> tokenAfterSteaming = new List<string>();
            foreach (var token in newTokens)
            {
                tokenAfterSteaming.Add(steamer.StemWord(token));
            }
            return tokenAfterSteaming;
        }
        private static List<string> stopWordRemovel(string[] tokens)
        {
            List<string> newTokens = new List<string>();
            foreach (var token in tokens)
            {
                if (!containStopWord_BS(STOP_WORD_LIST, 0, STOP_WORD_LIST.Length - 1, token) && token.Trim().Length > 1)
                {
                    newTokens.Add(token);
                }
            }
            return newTokens;
        }
        static bool containStopWord_BS(string[] stopWords, int start, int end, string word)
        {
            if (end >= start)
            {
                int mid = start + (end - start) / 2;
                if (String.Compare(stopWords[mid], word) == 0)
                    return true;

                if (String.Compare(stopWords[mid], word) > 0)
                    return containStopWord_BS(stopWords, start, mid - 1, word);

                return containStopWord_BS(stopWords, mid + 1, end, word);
            }

            return false;
        }
        private static string[] caseFolding(string[] tokens)
        {
            for (int i = 0; i < tokens.Length; i++)
            {
                tokens[i] = tokens[i].ToLower();
            }
            return tokens;
        }
        private static string[] getTokens(StringBuilder sb)
        {
            //normalization
            string temp = new Regex("[.]").Replace(getTextContent(sb), "");
            //tokenization
            string str = new Regex("[^a-zA-Z]").Replace(temp, " ");
            for (int i = 1; i < str.Length; i++)
            {
                if (Char.IsUpper(str[i]) && Char.IsLower(str[i - 1]))
                    str = str.Insert(i, " ");

            }

            string[] s = str.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            return s;
        }
        static string getTextContent(StringBuilder htmlString)
        {
            StringBuilder str = new StringBuilder();
            HtmlDocument doc = new HtmlDocument();
            doc.LoadHtml(htmlString.ToString());

            foreach (HtmlNode node in doc.DocumentNode.ChildNodes)
            {
                str.AppendLine(node.InnerText.TrimStart().TrimEnd().Replace("\n", string.Empty)
                    .Replace("\t", ""));
            }
            return str.ToString();
        }



        //first module
        public static void initDocDB()
        {
            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();

            //[1] init urls
            string url1 = "https://edition.cnn.com/";
            string url2 = "https://www.bbc.com/";
            string url3 = "https://www.wikipedia.org/";

            connection = new SqlConnection(connStr);
            connection.Open();
            List<String> db_links = new List<string>();
            SqlCommand cmd = new SqlCommand("select lnk from docTable", connection);
            cmd.CommandType = CommandType.Text;

            SqlDataReader reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                db_links.Add(reader["lnk"].ToString());
            }
            reader.Close();
            //db_links
            connection.Close();

            //threads
            Thread thread1 = new Thread(() => WepCrawling(url1, db_links, 1)) { IsBackground = true };
            thread1.Start();
            Thread thread2 = new Thread(() => WepCrawling(url2, db_links, 2)) { IsBackground = true };
            thread2.Start();
            Thread thread3 = new Thread(() => WepCrawling(url3, db_links, 3)) { IsBackground = true };
            thread3.Start();
            int i = 0;
            while (thread1.IsAlive || thread2.IsAlive || thread3.IsAlive)
            {
                Thread.Sleep(1000);
                Console.Clear();
                Console.WriteLine(i++);
            }

            if (!thread1.IsAlive && !thread2.IsAlive && !thread3.IsAlive)
            {
                if (connection.State == ConnectionState.Closed)
                    connection.Open();
                Console.WriteLine("*********************************** Thread 1 Urls ***********************************");
                foreach (KeyValuePair<string, string> entry in url_Doc_dict2)
                    Console.WriteLine(entry.Key);
                Console.WriteLine("*********************************** Thread 2 Urls ***********************************");
                foreach (KeyValuePair<string, string> entry in url_Doc_dict2)
                    Console.WriteLine(entry.Key);
                Console.WriteLine("*********************************** Thread 3 Urls ***********************************");
                foreach (KeyValuePair<string, string> entry in url_Doc_dict3)
                    Console.WriteLine(entry.Key);

                url_Doc_dict2.ToList().ForEach(pair => url_Doc_dict1[pair.Key] = pair.Value);
                url_Doc_dict3.ToList().ForEach(pair => url_Doc_dict1[pair.Key] = pair.Value);

                Console.WriteLine("*********************************** ALL Urls ***********************************");
                foreach (KeyValuePair<string, string> entry in url_Doc_dict1)
                {
                    Console.WriteLine(entry.Key);
                    insertDOCS(entry.Value, entry.Key);
                }

                if (connection.State == ConnectionState.Open)
                    connection.Close();
            }

            Console.WriteLine("Done");

            watch.Stop();
            Console.WriteLine($"Execution Time: {watch.ElapsedMilliseconds / 1000} s");
        }
        private static void WepCrawling(string url, List<string> db_links, int thread)
        {
            int insertCount = 0;
            List<String> ignore = new List<string>();

            if (db_links.Count == 0 || !db_links.Contains(url))
            {
                Queue<string> qUrls = new Queue<string>(storedSize);
                qUrls.Enqueue(url);

                while (qUrls.Count != 0)
                {
                    //[2] fetch page
                    string qUrl = qUrls.Dequeue();
                    WebRequest myWebRequest = WebRequest.Create(qUrl);

                    try
                    {
                        WebResponse myWebResponse = myWebRequest.GetResponse();
                        Stream streamResponse = myWebResponse.GetResponseStream();
                        StreamReader sReader = new StreamReader(streamResponse);
                        string rString = sReader.ReadToEnd();

                        //assert english only
                        string htmlTag = rString.Substring(0, 500);
                        if (htmlTag.Contains("lang"))
                        {
                            int index = htmlTag.IndexOf("lang");
                            string ch = htmlTag.Substring(index, 5);
                            if (ch.Contains("en"))
                            {
                                try
                                {
                                    if (storedSize <= insertCount)
                                    {
                                        streamResponse.Close();
                                        sReader.Close();
                                        myWebResponse.Close();
                                        goto End;
                                    }
                                    else
                                    {
                                        if (!db_links.Contains(qUrl))
                                        {
                                            Console.WriteLine(qUrl);
                                            insertCount++;
                                            if (thread == 1)
                                                url_Doc_dict1.Add(qUrl, rString);
                                            else if (thread == 2)
                                                url_Doc_dict2.Add(qUrl, rString);
                                            else
                                                url_Doc_dict3.Add(qUrl, rString);

                                        }
                                    }
                                }
                                catch (Exception e) { Console.WriteLine(e.Message); }
                            }
                        }
                        else
                        {
                            try
                            {
                                if (storedSize <= insertCount)
                                {
                                    streamResponse.Close();
                                    sReader.Close();
                                    myWebResponse.Close();
                                    goto End;
                                }
                                else
                                {
                                    if (!db_links.Contains(qUrl))
                                    {
                                        Console.WriteLine(qUrl);
                                        insertCount++;

                                        if (thread == 1)
                                            url_Doc_dict1.Add(qUrl, rString);
                                        else if (thread == 2)
                                            url_Doc_dict2.Add(qUrl, rString);
                                        else
                                            url_Doc_dict3.Add(qUrl, rString);
                                    }
                                }
                            }
                            catch (Exception e) { Console.WriteLine(e.Message); }
                        }

                        //[3] parse page

                        IHTMLDocument2 myDoc = new HTMLDocumentClass();
                        myDoc.write(rString);

                        IHTMLElementCollection elements = myDoc.links;

                        //[4] extract urls
                        foreach (IHTMLElement el in elements)
                        {
                            string link = (string)el.getAttribute("href", 0);
                            if (!ignore.Contains(link))
                            {
                                ignore.Add(link);
                                if (link.StartsWith("http"))
                                {
                                    if (qUrls.Count == storedSize)
                                        break;

                                    if (db_links.Contains(link))
                                        continue;
                                    qUrls.Enqueue(link);

                                }
                            }

                        }


                    }
                    catch (Exception e) { continue; }

                }
            End:
                Console.WriteLine("Thread number " + thread + " Done");
            }
            else
                Console.WriteLine("This Url is already exist");


        }
        private static void insertDOCS(string rString, string Url)
        {
            String query = "INSERT INTO dbo.docTable VALUES (@doc, @lnk)";
            SqlCommand cmdInsert = new SqlCommand(query, connection);
            // do something with entry.Value or entry.Key
            cmdInsert.Parameters.AddWithValue("@doc", rString);
            cmdInsert.Parameters.AddWithValue("@lnk", Url);
            cmdInsert.ExecuteNonQuery();
        }

    }
}
