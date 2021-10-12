using System;
using Marten;

namespace vac_seen_rollup
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Beginning daily rollup for location: us");

            int vaxcount = 0;
            var yesterday = DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd");
            // Get a count of vaccinations for the location ("us") for yesterday.
            Console.WriteLine("{0} vaccinations for {1}", vaxcount, yesterday);

            // Get count by reading Marten event database
            // Open a session for querying, loading, and
            // updating documents
            DocumentStore docstore = DocumentStore.For(Environment.GetEnvironmentVariable("ConnectionString"));

            // Open a session for querying, loading, and
            // updating documents with a backing "Identity Map"
            using (var session = docstore.QuerySession())
            {
                var existing = session
                    .Query<VaccinationEvent>()
                    .SingleAsync(x => x.CountryCode == "us" && x.ShotNumber < 4);
                Console.WriteLine(existing.ToString());
            }

            // UPSERT MariaDB database
        }
    }
}
