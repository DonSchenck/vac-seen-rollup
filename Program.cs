﻿using System;
using Marten;
using vac_seen_todb;
using System.Linq;
using System.Threading;
using MySqlConnector;
using System.Collections.Generic;
using System.Threading.Tasks;

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
            // Get a count of vaccinations for the location ("us").
            string countryCode = "US";

            // Get count by reading Marten event database
            // Open a session for querying, loading, and
            // updating documents
            try {
                // Update for the past 30 days
                for (int i = 30; i >= 0; i--)
                {
                    string yyyymmdd = DateTime.Now.AddDays(i).ToString("yyyyMMdd");
                    Console.WriteLine("Today is {0}", yyyymmdd);

                    int countForDate = 0;
                    //string sampleMartenConnectionString = "host=postgresql;username=postgres;password=7f986df431344327b52471df0142e520;";
                    string cs = Environment.GetEnvironmentVariable("MARTEN_CONNECTION_STRING");
                    Console.WriteLine("Connecting to Marten event database using this string: {0}", cs);
                    DocumentStore docstore = DocumentStore.For(cs);

                    // Open a session for querying
                    using (IDocumentSession session = docstore.OpenSession())
                    {
                        DateTime dt = DateTime.Now.AddDays(i);

                        Console.WriteLine("About to query...");
                        Console.WriteLine("Year == {0}", dt.Year.ToString());
                        Console.WriteLine("Month == {0}", dt.Month.ToString());
                        Console.WriteLine("Day == {0}", dt.Day.ToString());
                        var events = session.Query<VaccinationEvent>().Where(x => x.CountryCode == countryCode && x.EventTimestamp.Year == dt.Year && x.EventTimestamp.Month == dt.Month && x.EventTimestamp.Day == dt.Day);
                        //var events = session.Query<VaccinationEvent>().Where(x => x.CountryCode == "US");

                        countForDate = events.Count();
                        Console.WriteLine("Query done, returning {0} objects for {1}.", countForDate, yyyymmdd);
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
            } catch (Exception) {
                throw;
            }
        }
    }
}
