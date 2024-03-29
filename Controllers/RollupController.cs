﻿using Microsoft.AspNetCore.Mvc;
using System;
using Marten;
using Remotion.Linq;
using System.Threading;
using MySqlConnector;
using System.Collections.Generic;
using System.Threading.Tasks;
using KubeServiceBinding;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;

namespace vac_seen_rollup.Controllers;

[ApiController]
[Route("[controller]")]
public class RollupController : Controller
{
    [HttpPost]
    public async Task<ActionResult<int>> RollupEvents([FromForm] DateTime dateToRollup)
    {
        string countryCode = "US";
        Console.WriteLine("REQUEST RECEIVED");

        // Get a count of vaccinations for the location.
        Console.WriteLine("Beginning daily rollup for location: {0}", countryCode);

        // Get count by reading Marten event database
        // Open a session for querying, loading, and
        // updating documents
        try
        {
            string yyyymmdd = dateToRollup.ToString("yyyyMMdd");
            Console.WriteLine("Rolling up for {0}", dateToRollup.ToString());

            int countForDate = 0;
            string cs = Environment.GetEnvironmentVariable("MARTEN_CONNECTION_STRING");
            Console.WriteLine("Connecting to Marten event database using this string: {0}", cs);
            DocumentStore docstore = DocumentStore.For(cs);

            // Open a session for querying
            using (IDocumentSession session = docstore.OpenSession())
            {
                DateTime dateToQuery = dateToRollup;
                DateTime dateAfterQuery = dateToQuery.AddDays(1);

                var from = new DateTimeOffset(dateToRollup);
                var to = from.AddDays(1).AddTicks(-1);

                Console.WriteLine("About to query...");
                Console.WriteLine("Country Code == {0}", countryCode);
                Console.WriteLine("Querying from {0} to {1}", from.ToString(), to.ToString());  
                //var events = await session.Query<VaccinationEvent>().Where(x => x.CountryCode == countryCode && x.EventTimestamp >= from && x.EventTimestamp <= to).ToListAsync();
                List<VaccinationEvent> events = session.Query<VaccinationEvent>().Where(x => x.CountryCode == countryCode && x.EventTimestamp >= from && x.EventTimestamp <= to).ToList();
                
                countForDate = events.Count;
                
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
