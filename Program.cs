using System;
using Marten;
using vac_seen_todb;
using System.Linq;
using System.Threading;
using MySqlConnector;

namespace vac_seen_rollup
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Beginning daily rollup for location: us");
            Rollup();
        }
        static void Rollup() {
            int vaxcount = 0;
            var yesterday = DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd");
            // Get a count of vaccinations for the location ("us") for yesterday.
            string countryCode = "us";
            Console.WriteLine("{0} vaccinations for {1}", vaxcount, yesterday);

            // Get count by reading Marten event database
            // Open a session for querying, loading, and
            // updating documents
            try {
                int countForYesterday = 0;
                string cs = Environment.GetEnvironmentVariable("ConnectionString");
                Console.WriteLine("Connecting using this string: {0}",cs);
                DocumentStore docstore = DocumentStore.For(cs);

                // Open a session for querying
                using (IDocumentSession session = docstore.OpenSession())
                {
                    Console.WriteLine("About to query...");
                    //var events = session.Query<VaccinationEvent>().Take(10);
                    var events = session.Query<VaccinationEvent>();
                    countForYesterday = events.Count(); 
                    Console.WriteLine("Query done, returning {0} objects.", countForYesterday);
                }

                // UPSERT MariaDB database
                string insert = string.Format("INSERT INTO vaccination_summaries (location_code,reporting_date,vaccination_count) VALUES('{0}',{1},{2})", countryCode, yesterday, countForYesterday);
                Console.WriteLine("Updating vaccination_summaries with this statement: {0}", insert);

                using (var connection = new MySqlConnection("Server=mysql;User ID=root;Password=admin;Database=vaxdb"))
                {
                    connection.Open();

                    using (var command = new MySqlCommand(insert, connection))
                        command.ExecuteNonQueryAsync();
                }

                Thread.Sleep(60000);

            } catch (Exception e) {
                throw e;
            }
        }
    }
}
