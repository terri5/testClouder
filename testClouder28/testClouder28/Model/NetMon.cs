using analyzeLogWorkRole.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using AlalyzeLog.DBTools;
using System.Text.RegularExpressions;

namespace testClouder28.Model
{
    class NetMon : IHBaseModel
    {

        public const string TABLE_NAME = "T_DEVICE_LOG_PV";
        public const string COLUMN_FAMILY = "PV";
        public const string ROW_KEY = "ROWKEY";
        public const string DMAC = "dmac";
        public const string MAC = "mac";
        public const string CLIENT_AGENT = "clientAgent";
        public const string HTTP_URI = "httpUri";
        public const string HTTP_METHOD = "httpMethod";
        public const string HTTP_VERSION = "httpVersion";
        public const string HTTP_HOST = "httpHost";
        public const string IP = "ip";
        public const string DATETIME_POINT = "DateTime_Point";
        public const string DAY_ID = "day_id";
        public const string IN_DB_DATETIME = "INDB_DATETIME";
        public const string REFERER = "referer";
        public const string CLIENT_OS = "client_os";
        public const string MOBILE_BRAND = "mobile_brand";
        public const string CLIENT_BROWSER = "client_browser";
        public readonly int BATHCH = 5000;

        private ConcurrentQueue<string> dwFileQueue = new ConcurrentQueue<string>();


        private long cnt = 0;
        private long RCnt = 0; //去重前的记录数
        public void AddRCnt1()
        {
            Interlocked.Increment(ref RCnt);
        }

        public void AddCnt(int cnt1)
        {
            Interlocked.Add(ref cnt, cnt1);
        }

        public void AddRCnt(int cnt1)
        {
            Interlocked.Add(ref RCnt, cnt1);
        }


        public void BsonToDw(BsonDocument doc, StringBuilder sb)
        {
            sb.Append(doc.GetValue(DMAC, "")).Append("\t")
                                     .Append(doc.GetValue(MAC, "")).Append("\t")
                                     .Append(doc.GetValue(IP, "")).Append("\t")
                                     .Append(DateTime.Parse(doc.GetValue(DATETIME_POINT).AsString).ToString("yyyy-MM-dd HH:mm:ss")).Append("\t")
                                     .Append(StringUtil.GetTrimLengthString(doc.GetValue(CLIENT_AGENT,"").AsString, 8000)).Append("\t")
                                     .Append(doc.GetValue(HTTP_METHOD, "")).Append("\t")
                                     .Append(StringUtil.GetTrimLengthString(doc.GetValue(HTTP_URI).AsString, 8000)).Append("\t")
                                     .Append(doc.GetValue(HTTP_VERSION, "")).Append("\t")
                                     .Append(doc.GetValue(REFERER, "")).Append("\t")
                                     .Append(doc.GetValue(CLIENT_OS, "")).Append("\t")
                                     .Append(doc.GetValue(MOBILE_BRAND, "")).Append("\t")
                                     .Append(doc.GetValue(CLIENT_BROWSER, "")).Append("\t")
                                     .Append(doc.GetValue(DAY_ID, "")).Append("\t")
                                     .Append(doc.GetValue(IN_DB_DATETIME, "")).Append("\t")
                                     .Append(DateTime.Now.ToString("yyyyMMddHHmmss")).Append("\t")
                                     .Append(doc.GetValue(HTTP_HOST, "")).AppendLine();

        }

        public string GetColumnFamily()
        {
            throw new NotImplementedException();
        }

        public ConcurrentQueue<string> GetDwFileQueue()
        {
            return dwFileQueue;
        }

        public string getDwTable()
        {
            throw new NotImplementedException();
        }

        public string GetTableName()
        {
            throw new NotImplementedException();
        }

        public long GetRCnt()
        {
            return RCnt;
        }
        public long GetCnt()
        {
            return cnt;
        }
        public BsonDocument LineToBson(string line, string dmac)
        {
            if (string.IsNullOrEmpty(line))
                return null;
            string[] fields = Regex.Replace(line, " {2,}", " ").Split(' ');
            BsonDocument bson = new BsonDocument();

            int ix, jx;
            // 把非空的字段集中到一起


            int fieldCount;
            for (fieldCount = 0; fieldCount < fields.Length && fields[fieldCount] != null && fields[fieldCount].Trim().Length != 0; fieldCount++) ;
            if (fieldCount != 8 && fieldCount != 9 || !fields[5].Equals(">"))
            {
                return null;
            }
            DateTime date = DateTime.Parse(fields[0] + " " + fields[1]);
            /*
            try
            {
                date = DateTime.Parse(fields[0] + " " + fields[1]);
            }
            catch (Exception e) {

            }
            */
            if (date == null)
                return null;

            String umac = fields[2];
            if (umac.Length == 16)
                umac = "0" + umac;
            if (umac.Length != 17)
            {
                return null; ;
            }
            umac = umac.ToLower();

            // 分析URI
            String uri = "";
            String host = "";

            if (fieldCount == 9)
            {
                host = fields[7];
                uri = fields[8];
                if (uri.StartsWith("http://"))
                    uri = uri.Substring(7);
                if (uri.StartsWith(host))
                    uri = uri.Substring(host.Length);
            }
            else
            {
                uri = fields[7];
                ix = uri.IndexOf("/");
                if (ix < 0)
                {
                    return null;
                }

                host = uri.Substring(0, ix);
                if (!host.Contains("."))
                {
                    host = string.Empty;
                }
                uri = uri.Substring(ix);
            }
            if (string.IsNullOrEmpty(uri))
                return null;
            string ip = fields[3];
            string httpMethod = fields[6];

            bson.Add(DATETIME_POINT, date.ToString("yyyy-MM-dd HH:mm:ss"));
            bson.Add(DAY_ID, date.ToString("yyyyMMdd"));
            bson.Add(HTTP_HOST, host);
            bson.Add(HTTP_URI, uri);
            bson.Add(MAC, umac);
            bson.Add(IP, ip);
            bson.Add(HTTP_METHOD, httpMethod);
            bson.Add(DMAC, dmac);
            bson.Add(IN_DB_DATETIME, ConvertUtil.LocalToUTC8(DateTime.Now).ToString("yyyyMMddHHmmss"));
            bson.Add(ConvertUtil.Unique_Key, dmac + date.ToString("yyyyMMddHHmm").Substring(0, 11) + ip.Replace(".", "") + StringUtil.ConvertBase64(uri));
            string rowkey = ConvertUtil.getHbaseRowKeyUnique(date, dmac);
            bson.Add(ROW_KEY, rowkey);
            /*
            if (host.Contains("amol.com.cn"))   // 航美内部服务，不算外网访问
                continue;
                */


            return bson;
        }

        public void SetDataRow(DataTable dt, BsonDocument data)
        {
            throw new NotImplementedException();
        }

        public void SetDataTableColumnsFromDB(DataTable dt, SqlConnection conn, string tabName)
        {
            throw new NotImplementedException();
        }

        public bool shouldInToDB(int count)
        {
            return count >= BATHCH;
        }

        public CellSet.Row ToRowOfCellSet(BsonDocument model)
        {
            throw new NotImplementedException();
        }

        public void AddCnt1()
        {
            Interlocked.Increment(ref cnt);
        }
    }
}
