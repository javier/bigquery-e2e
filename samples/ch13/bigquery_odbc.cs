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
    private String Dsn;
    private String Catalog;
    private String ExecCatalog;
    private bool UseNativeQuery;

    public ConnectionBuilder SetDsn(String dsn) { 
      Dsn = dsn; 
      return this; 
    }
    public ConnectionBuilder SetCatalog(String catalog) {
      Catalog = catalog; 
      return this; 
    }
    public ConnectionBuilder SetBillingCatalog(String catalog) {
      ExecCatalog = catalog;
      return this;
    }
    public ConnectionBuilder SetUseNativeQuery(bool nativeQuery) {
      UseNativeQuery = nativeQuery;
      return this;
    }

    public OdbcConnection Build() {
      if (Catalog == null || Dsn == null) {
        throw new ArgumentException("Missing required Connection setting");
      }

      StringBuilder connectionString = new StringBuilder();

      connectionString.AppendFormat("DSN={0}; Catalog={1};", Dsn, Catalog);
      if (ExecCatalog != null) {
        connectionString.AppendFormat("ExecCatalog={0};", ExecCatalog);
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
   * Simple console program that runs a query against BigQuery, prints the results,
   * and waits for a user to hit any key before exiting.
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
      OdbcConnection connection = new ConnectionBuilder()
          .SetDsn("bigquery1")
          .SetCatalog("publicdata")
          .SetBillingCatalog("bigquery-e2e")
          .Build();
      try {
        connection.Open();
        using (OdbcCommand command =  connection.CreateCommand()) {
          command.CommandText = Query;
          using (OdbcDataReader reader = command.ExecuteReader()) {
            PrintResults(reader);
          }
        }
      } catch (Exception ex) {
        System.Console.WriteLine("Error {0}: {1}",
          connection.State != ConnectionState.Open ? "opening connection" : "executing query",
          ex);
      } finally {
        connection.Close();
      }
      System.Console.ReadKey();
    }
  }
}
