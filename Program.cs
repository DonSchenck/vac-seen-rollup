﻿using System;
using Marten;
using vac_seen_todb;
using System.Linq;
using System.Threading;

namespace vac_seen_rollup
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Beginning daily rollup for location: us");
            Rollup();
        }
        async static void Rollup() {
            int vaxcount = 0;
            var yesterday = DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd");
            // Get a count of vaccinations for the location ("us") for yesterday.
            Console.WriteLine("{0} vaccinations for {1}", vaxcount, yesterday);

            // Get count by reading Marten event database
            // Open a session for querying, loading, and
            // updating documents
            try {
                string cs = Environment.GetEnvironmentVariable("ConnectionString");
                Console.WriteLine("Connecting using this string: {0}",cs);
                DocumentStore docstore = DocumentStore.For(cs);

                // Open a session for querying
                using (IDocumentSession session = docstore.OpenSession())
                {
                    Console.WriteLine("About to query...");
                    //var events = session.Query<VaccinationEvent>().Take(10);
                    var events = session.Query<VaccinationEvent>();

                    Console.WriteLine("Query done, returning {0} objects.", events.Count());
                    foreach(VaccinationEvent e in events)
                    {
                        //Console.WriteLine("Vaccination Event Id: {0}", e.Id);
                    }
                    Thread.Sleep(60000);
                }

                // UPSERT MariaDB database
            } catch (Exception e) {
                throw e;
            }
        }
    }
}
