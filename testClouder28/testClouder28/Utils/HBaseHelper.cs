using Microsoft.HBase.Client;
using Microsoft.HBase.Client.LoadBalancing;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AnlalyzeLog.DBTools
{
    class HBaseHelper
    {
        static string clusterURL = "https://airmediahbasepro.azurehdinsight.cn";
        static string httpName = "airmediahbaseprohttp";
        static string httpUserPassword = "Password!23";

        public static HBaseClient hbaseClient = null;

        static HBaseHelper()
        {
            InitHBase();
        }

        private static void InitHBase()
        {
            try
            {
                clusterURL = System.Configuration.ConfigurationManager.AppSettings["HBaseClusterURL"].ToString();
                httpName = System.Configuration.ConfigurationManager.AppSettings["HBaseHttpName"].ToString();
                httpUserPassword = System.Configuration.ConfigurationManager.AppSettings["HBaseHttpUserPassword"].ToString();
                hbaseClient = CreateHBaseClient(clusterURL, httpName, httpUserPassword);
            }
            catch (Exception e)
            {

                Console.Error.WriteLine(e.Message + "\r\n" + e.StackTrace);
                Console.ReadKey();
            }
            
        }

        /// <summary>
        /// Create a new instance of an HBase client.
        /// </summary>
        /// <param name="clusterURL"></param>
        /// <param name="httpName"></param>
        /// <param name="httpUserPassword"></param>
        /// <returns></returns>
        private static HBaseClient CreateHBaseClient(string clusterURL, string httpName, string httpUserPassword)
        {
            ClusterCredentials creds = new ClusterCredentials(new Uri(clusterURL), httpName, httpUserPassword);
            return new HBaseClient(creds);
        }

        /// <summary>
        /// Create a new HBase table
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public static bool CreateTable(string tableName, string[] columnFamilysKeys)
        {
            TableSchema tableSchema = new TableSchema();
            tableSchema.name = tableName;
            foreach (var columnFamilySKey in columnFamilysKeys)
            {
                tableSchema.columns.Add(new ColumnSchema() { name = columnFamilySKey });
            }

            if (hbaseClient == null)
                hbaseClient = CreateHBaseClient(clusterURL, httpName, httpUserPassword);

            int millisecondsTimeout = 3000;
            return hbaseClient.CreateTableAsync(tableSchema).Wait(millisecondsTimeout);

        }

        /// <summary>
        /// Create a new HBase table by async
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public static async Task<bool> CreateTableAsync(string tableName, string[] columnFamilysKeys)
        {
            TableSchema tableSchema = new TableSchema();
            tableSchema.name = tableName;
            foreach (var columnFamilySKey in columnFamilysKeys)
            {
                tableSchema.columns.Add(new ColumnSchema() { name = columnFamilySKey });
            }

            if (hbaseClient == null)
                hbaseClient = CreateHBaseClient(clusterURL, httpName, httpUserPassword);

            return await hbaseClient.CreateTableAsync(tableSchema);
        }

        /// <summary>
        /// Scan over rows in a table. Assume the table has integer keys and you want data between keys startRow and startRow.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="startRow"></param>
        /// <param name="endRow"></param>
        public static async Task Scan(string tableName, int startRow, int endRow)
        {
            Scanner scanSettings = new Scanner()
            {
                batch = 10000,
                startRow = BitConverter.GetBytes(startRow),
                endRow = BitConverter.GetBytes(endRow)
            };

            if (hbaseClient == null)
                hbaseClient = CreateHBaseClient(clusterURL, httpName, httpUserPassword);

            RequestOptions requestOptions = RequestOptions.GetDefaultOptions();
            requestOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;

            ScannerInformation scannerInfo = await hbaseClient.CreateScannerAsync(tableName, scanSettings, RequestOptions.GetDefaultOptions());
            CellSet next = null;

            while ((next = await hbaseClient.ScannerGetNextAsync(scannerInfo, RequestOptions.GetDefaultOptions())) != null)
            {
                foreach (CellSet.Row row in next.rows)
                {
                    Console.WriteLine(row.key + " : " + Encoding.UTF8.GetString(row.values[0].data));
                }
            }
        }

        /// <summary>
        /// delete table HBase
        /// </summary>
        /// <param name="tableName"></param>
        public static async Task DeleteAsync(string tableName)
        {
            if (hbaseClient == null)
                hbaseClient = CreateHBaseClient(clusterURL, httpName, httpUserPassword);

            await hbaseClient.DeleteTableAsync(tableName, RequestOptions.GetDefaultOptions());
        }

        /// <summary>
        /// batch insert data to hbase
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="cellSetRows"></param>
        /// <param name="num"></param>
        public static async Task HbaseBatchInsertAsync(string tableName, List<CellSet.Row> cellSetRows)
        {
            CellSet cellSet = new CellSet();
            foreach (var cellSetRow in cellSetRows)
            {
                cellSet.rows.Add(cellSetRow);
            }

            await HbaseBatchInsertAsync(tableName, cellSet);

        }

        /// <summary>
        /// batch insert data to hbase
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="cellSet"></param>
        /// <param name="num"></param>
        /// <returns></returns>
        public static async Task HbaseBatchInsertAsync(string tableName, CellSet cellSet)
        {
            if (hbaseClient == null)
                hbaseClient = CreateHBaseClient(clusterURL, httpName, httpUserPassword);

            await hbaseClient.StoreCellsAsync(tableName, cellSet);

        }

    }
}
