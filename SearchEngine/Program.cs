using System;
using System.Collections.Generic;


namespace ConsoleApp1
{
    class Program
    {

        static void Main(string[] args)
        {
            WepCrawler.initDocDB(); 
            Console.Clear();
            WepCrawler.initIndexDB();
            Console.WriteLine("DONE"); //champions league
            string query1 = Console.ReadLine();
            List<string> result_links = WepCrawler.Search(query1);
            if (result_links.Count == 0)
            {
                Console.WriteLine("No result found...");
            }
            else
            {
                foreach (var link in result_links)
                {
                    Console.WriteLine(link);
                }
            }
        }
    }

}
