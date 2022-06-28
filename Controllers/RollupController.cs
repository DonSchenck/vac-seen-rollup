using Microsoft.AspNetCore.Mvc;
using System;
using Marten;
using System.Linq;
using System.Threading;
using MySqlConnector;
using System.Collections.Generic;
using System.Threading.Tasks;
using KubeServiceBinding;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;

namespace vac_seen_rollup.Controllers;

public class RollupController : Controller
{
    [HttpPost]
    public async Task<ActionResult<int>> RollupEvents([FromForm] DateTime dateToRollup, string countryCode)
    {
        Console.WriteLine("REQUEST RECEIVED");

        // Get a count of vaccinations for the location.
        Console.WriteLine("Beginning daily rollup for location: {0}", countryCode);

        // Get count by reading Marten event database
        // Open a session for querying, loading, and
        // updating documents
        try
        {
            string yyyymmdd = dateToRollup.ToString("yyyyMMdd");
            Console.WriteLine("Rolling up for {0}", yyyymmdd);

            int countForDate = 0;
            string cs = Environment.GetEnvironmentVariable("MARTEN_CONNECTION_STRING");
            Console.WriteLine("Connecting to Marten event database using this string: {0}", cs);
            DocumentStore docstore = DocumentStore.For(cs);

            // Open a session for querying
            using (IDocumentSession session = docstore.OpenSession())
            {
                DateTime dateToQuery = dateToRollup;
                DateTime dateAfterQuery = dateToQuery.AddDays(i + 1);

                Console.WriteLine("About to query...");
                Console.WriteLine("Date == {0}", dateToQuery);
                Console.WriteLine("Date After == {0}", dateAfterQuery);

                var events = session.Query<VaccinationEvent>().Where(x => x.CountryCode == countryCode && x.EventTimestamp >= dateToQuery && x.EventTimestamp < dateAfterQuery);
                countForDate = events.Count();
                Console.WriteLine("Query done, returning {0} objects for {1} for location {2}.", countForDate, yyyymmdd, countryCode);
            }

            // UPSERT MariaDB database
            string upsertCmd = string.Format("INSERT INTO vaccination_summaries (location_code,reporting_date,vaccination_count) VALUES('{0}','{1}',{2}) ON DUPLICATE KEY UPDATE vaccination_count = {3}", countryCode, yyyymmdd, countForDate, countForDate);
            Console.WriteLine("Updating table vaccination_summaries with this statement: {0}", upsertCmd);
            //string sampleMySqlConnectionString = "Server=mysql;User ID=root;Password=admin;Database=vaxdb";
            using (var connection = new MySqlConnection(Environment.GetEnvironmentVariable("MYSQL_CONNECTION_STRING")))
            {
                connection.Open();
                using (var command = new MySqlCommand(upsertCmd, connection))
                    command.ExecuteNonQuery();
            }
        }
        catch (Exception)
        {
            throw;
        }

        // Return count
        return 0;
    }

    private static Dictionary<string, string> GetDotnetServiceBindings()
    {
        int count = 0;
        int maxTries = 999;
        while (true)
        {
            try
            {
                DotnetServiceBinding sc = new DotnetServiceBinding();
                Dictionary<string, string> d = sc.GetBindings("kafka");
                return d;
                // At this point, we have the information needed to bind to our Kafka
                // bootstrap server.
            }
            catch (Exception e)
            {
                // handle exception
                Console.WriteLine("Waiting for service binding...");
                System.Threading.Thread.Sleep(1000);
                if (++count == maxTries) throw e;
            }
        }
    }

    public static SecurityProtocol ToSecurityProtocol(string bindingValue) => bindingValue switch
    {
        "SASL_SSL" => SecurityProtocol.SaslSsl,
        "PLAIN" => SecurityProtocol.Plaintext,
        "SASL_PLAINTEXT" => SecurityProtocol.SaslPlaintext,
        "SSL" => SecurityProtocol.Ssl,
        _ => throw new ArgumentOutOfRangeException(bindingValue, $"Not expected SecurityProtocol value: {bindingValue}"),
    };
    public static SaslMechanism ToSaslMechanism(string bindingValue) => bindingValue switch
    {
        "GSSAPI" => SaslMechanism.Gssapi,
        "PLAIN" => SaslMechanism.Plain,
        "SCRAM-SHA-256" => SaslMechanism.ScramSha256,
        "SCRAM-SHA-512" => SaslMechanism.ScramSha512,
        _ => throw new ArgumentOutOfRangeException(bindingValue, $"Not expected SaslMechanism value: {bindingValue}"),
    };
}
