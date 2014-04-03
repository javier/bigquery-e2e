/**
 * All rights to this package are hereby disclaimed and its contents
 * released into the public domain by the authors.
*/
/**
 * Simple Progrm to demonstrate usage of BigQuery through ODBC.
 * Requries a BigQuery DSN named 'bigquery1' to have been set up.
 */
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQueryE2E
{
  /** 
   * Helper class to build an ODBC Connection to connect to a Simba 
   * BigQuery ODBC Driver.
   */
  class ConnectionBuilder {
    public String Dsn;
    public String Catalog;
    public String ExecCatalog;
    public bool UseNativeQuery;

    public OdbcConnection Build() {
      if (Catalog == null || Dsn == null) {
        throw new ArgumentException(
            "Missing required Connection setting");
      }

      StringBuilder connectionString = new StringBuilder();
      connectionString.AppendFormat("DSN={0}; Catalog={1};", 
          Dsn, Catalog);
      if (ExecCatalog != null) {
        connectionString.AppendFormat("ExecCatalog={0};", 
            ExecCatalog);
      }
      if (UseNativeQuery) {
        connectionString.Append("UseNativeQuery=1");
      }

      OdbcConnection conn = new OdbcConnection();
      conn.ConnectionString = connectionString.ToString();
      return conn;
    }
  }

  /** 
   * Simple console program that runs a query against BigQuery, 
   * prints the results, and waits for a user to hit any key 
   * before exiting.
   */
  class Program {
    private static String Query = 
        "SELECT corpus, SUM(word_count) " + 
        "FROM samples.shakespeare " +
        "GROUP BY corpus";

    private static void PrintResults(OdbcDataReader reader) {
      for (int ii = 0; ii < reader.FieldCount; ii += 1) {
        System.Console.Write("{0}{1}",
            reader.GetName(ii),
            ii + 1 < reader.FieldCount ? "\t" : "\n");
      }
      while (reader.Read()) {
        for (int ii = 0; ii < reader.FieldCount; ii += 1) {
          System.Console.Write("{0}{1}",
              reader.GetValue(ii),
              ii + 1 < reader.FieldCount ? "\t" : "\n");
        }
      }
    }

    static void Main(string[] args) {
      ConnectionBuilder builder = new ConnectionBuilder();
      // Set this to the name of the ODBC dns you created:
      builder.Dsn = "bigquery1";
      // This is the default project that will be used to resolve tables 
      // in the job:
      builder.Catalog = "publicdata";
      // Set this to your own project ID so that Jobs are run under
      // this project:
      builder.ExecCatalog = "bigquery-e2e";

      string state = "creating connection";
      try {
        state = "opening connection";
        using (OdbcConnection connection = builder.Build()) {
          connection.Open();
          state = "creating command";
          using (OdbcCommand command = connection.CreateCommand()) {
            command.CommandText = Query;
            state = "running query";
            using (OdbcDataReader reader = command.ExecuteReader()) {
              PrintResults(reader);
            }
          }
        }
      } catch (Exception ex) {
        System.Console.WriteLine("Error {0}: {1}", state, ex);
      }
      // Wait until the "any key" is pressed.
      System.Console.ReadKey();
    }
  }
}

