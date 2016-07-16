using AlalyzeLog.DBTools;
using analyzeLogWorkRole.Model;
using MongoDB.Bson;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AlalyzeLog.Worker.Model
{
    public class UV : IHBaseModel
    {

        public const string HBASE_TABLE = "DEVICE_LOG_UV";
        public const string COLUMN_FAMILY = "UV";
        public const string ROW_KEY = "ROWKEY";
        public const string DMAC = "dmac";
        public const string MAC = "mac";
        public const string IP = "ip";
        public const string STARTTIME = "starttime";
        public const string ENDTIME = "endtime";
        public const string WIFIUP = "wifiup";
        public const string WIFIDOWN = "wifidown";
        public const string UP3G = "UP3G";
        public const string DOWN3G = "DOWN3G";
        public const string DAY_ID = "day_id";
        public const string INDB_DATETIME = "INDB_DATETIME";

        private readonly int BATHCH=1000;

        private ConcurrentQueue<string> dwFileQueue = new ConcurrentQueue<string>();

        private long cnt = 0;
        private long RCnt = 0; //去重前的记录数


        public CellSet.Row ToRowOfCellSet(BsonDocument uv)
        {
            CellSet.Row cellSetRow = new CellSet.Row() { key = Encoding.UTF8.GetBytes(uv.GetValue(ROW_KEY).AsString) };
            try
            {

                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + DMAC.ToUpper()), data = Encoding.UTF8.GetBytes(uv.GetValue(DMAC).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + MAC.ToUpper()), data = Encoding.UTF8.GetBytes(uv.GetValue(MAC, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + IP.ToUpper()), data = Encoding.UTF8.GetBytes(uv.GetValue(IP, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + STARTTIME.ToUpper()), data = Encoding.UTF8.GetBytes(uv.GetValue(STARTTIME, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + ENDTIME.ToUpper()), data = Encoding.UTF8.GetBytes(uv.GetValue(ENDTIME, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + WIFIUP.ToUpper()), data = Encoding.UTF8.GetBytes(Convert.ToInt64(uv.GetValue(WIFIUP).ToString()) + "") });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + WIFIDOWN.ToUpper()), data = Encoding.UTF8.GetBytes(Convert.ToInt64(uv.GetValue(WIFIDOWN).ToString()) + "") });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + UP3G.ToUpper()), data = Encoding.UTF8.GetBytes(Convert.ToInt64(uv.GetValue("3Gup").ToString()) + "") });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + DOWN3G.ToUpper()), data = Encoding.UTF8.GetBytes(Convert.ToInt64(uv.GetValue("3Gdown").ToString()) + "") });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + DAY_ID.ToUpper()), data = Encoding.UTF8.GetBytes(uv.GetValue(DAY_ID).AsInt32 + "") });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + INDB_DATETIME.ToUpper()), data = Encoding.UTF8.GetBytes(uv.GetValue(INDB_DATETIME).AsInt64 + "") });
            }
            catch (Exception e)
            {
                Trace.TraceError("uv covert to hbase model failed:" + uv + "\r\n" + e.Message + "\n" + e.StackTrace);
                CommonUtil.LogException(uv.GetValue(DMAC).AsString, e);
                return null;
            }

            return cellSetRow;
        }

        public string GetTableName()
        {
            return HBASE_TABLE;
        }

        public string GetColumnFamily()
        {
            return COLUMN_FAMILY;
        }

        public BsonDocument LineToBson(string json, string dmac)
        {
            DateTime hostdate = ConvertUtil.LocalToUTC8(DateTime.Now);
            string json1 = json.Replace("bytes", "");
            BsonDocument data =ConvertUtil.GetBsonDocumentByString(json1, dmac);
            if (data == null) return null;
            string dateStr = data.GetValue(STARTTIME).AsString;
            string mac = data.GetValue(MAC).AsString;
            string id = dmac + StringUtil.FormatMacString(mac) + dateStr.Replace("-", "").Replace(" ", "").Replace(":", "");
            DateTime starttime = ConvertUtil.TrimDateMissing(dateStr);
            data.Add(DAY_ID, Convert.ToInt32(starttime.ToString("yyyyMMdd")));
            data.Add(INDB_DATETIME, long.Parse(hostdate.ToString("yyyyMMddHHmmss")));
            string rowkey = ConvertUtil.getHbaseRowKeyUnique(starttime, dmac);
            data.Add(ROW_KEY, rowkey);
            ConvertUtil.SafePutBsonValue(data, ConvertUtil.Unique_Key, id);
            return data;
        }


      
        public void BsonToDw(BsonDocument data, StringBuilder sb)
        {

            BsonDocument p = data;
            Nullable<DateTime> starttime = null;
            Nullable<DateTime> endtime = null;
            int day_id = 0;
            if (p.Contains("starttime") && !p.GetValue("starttime").IsBsonNull && p.GetValue("starttime").ToString().Trim().Length > 0)
            {
                //          Console.WriteLine("starttime:" + p.GetValue("starttime").ToString());
                try
                {
                    starttime = DateTime.ParseExact(p.GetValue("starttime").ToString(), "yyyy-MM-dd-HH:mm:ss", null);
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine("设备{0}的数据产生异常：{1}" + p.GetValue("dmac"), e.Message);
                    return;
                }

            }
            if (p.Contains("endtime") && !p.GetValue("endtime").IsBsonNull && p.GetValue("endtime").ToString().Trim().Length > 0)
            {
                //             Console.WriteLine("endtime:" + p.GetValue("endtime"));
                try
                {
                    endtime = DateTime.ParseExact(p.GetValue("endtime").ToString(), "yyyy-MM-dd-HH:mm:ss", null);
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine("设备{0}的数据产生异常：{1}" + p.GetValue("dmac"), e.Message);
                    return;

                }
            }
            if (starttime != null)
            {

                try
                {
                    day_id = int.Parse(starttime?.ToString("yyyyMMdd"));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine("设备{0}的数据产生异常：{1}", p.GetValue("dmac"), e.Message);
                    return;
                }
            }
            sb.Append(data.GetValue("dmac")).Append("\t").Append(data.GetValue("mac", "")).Append("\t")
               .Append(data.GetValue("IP", "")).Append("\t").Append(starttime == null ? "" : starttime?.ToString("yyyy-MM-dd HH:mm:ss")).Append("\t")
               .Append(endtime == null ? "" : endtime?.ToString("yyyy-MM-dd HH:mm:ss")).Append("\t").Append(data.GetValue("wifiup", "")).Append("\t")
               .Append(data.GetValue("wifidown", "")).Append("\t").Append(data.GetValue("3Gup", "")).Append("\t")
               .Append(data.GetValue("3Gdown")).Append("\t").Append(day_id).Append("\t")
               .Append(data.GetValue(INDB_DATETIME, "")).Append("\t")
               .Append(DateTime.Now.ToString("yyyyMMddHHmmss")).AppendLine();
        }
        public bool shouldInToDB(int count)
        {
            return count >= BATHCH;
        }
        public ConcurrentQueue<string> GetDwFileQueue()
        {
            return dwFileQueue;
        }

        public long GetCnt()
        {
            return cnt;
        }
        public long GetRCnt()
        {
            return RCnt;
        }

        public void AddCnt1()
        {
            Interlocked.Increment(ref cnt);
        }

        public void AddRCnt1()
        {
            Interlocked.Increment(ref RCnt);
        }


    }
}
