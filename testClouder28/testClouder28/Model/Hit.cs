using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System.Diagnostics;
using AlalyzeLog.DBTools;
using System.Collections.Concurrent;
using System.Threading;

namespace analyzeLogWorkRole.Model
{
    class Hit : IHBaseModel
    {
        public const string HBASE_TABLE = "DEV_HIT_BASE";
        public const string COLUMN_FAMILY = "HIT";
        public const string ROW_KEY = "ROWKEY";
        public const string DMAC = "dmac";
        public const string MAC = "mac";
        public const string IP = "ip";
        public const string TIME = "time";
        public const string HITID = "hitID";
        public const string REFHITID = "refHitID";
        public const string UID = "uID";
        public const string POSIDX = "posIdx";
        public const string PAGETIME = "pageTime";
        public const string DAY_ID = "day_id";
        public const string INDB_DATETIME = "INDB_DATETIME";
        private readonly int BATHCH = 1000;

        private ConcurrentQueue<string> dwFileQueue = new ConcurrentQueue<string>();

        private long cnt = 0;
        private long RCnt = 0; //去重前的记录数


        public CellSet.Row ToRowOfCellSet(BsonDocument hit)
        {
            CellSet.Row cellSetRow = new CellSet.Row() { key = Encoding.UTF8.GetBytes(hit.GetValue(ROW_KEY).AsString) };
            try
            {
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + DMAC.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(DMAC).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + MAC.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(MAC).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + IP.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(IP).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + TIME.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(TIME).AsInt64 + "") });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + HITID.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(HITID).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + REFHITID.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(REFHITID, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + UID.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(UID).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + POSIDX.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(POSIDX).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + PAGETIME.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(PAGETIME).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + DAY_ID.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(DAY_ID).AsInt32 + "") });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + INDB_DATETIME.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(INDB_DATETIME).AsInt64 + "") });
            }
            catch (Exception e)
            {
                Trace.TraceError("Hit covert to hbase model failed:" + hit + "\r\n" + e.Message + "\n" + e.StackTrace);
                CommonUtil.LogException(hit.GetValue(DMAC).AsString, e);
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

        public BsonDocument LineToBson(string line,string dmac)
        {
            string dateStr;
            int day_id = 0;

            long unixDate;
            DateTime start;

            //主机时间
            BsonDocument data=null;
            string rowkey = "";

            //获取数据，建立BsonDocument对象，并添加属性数据
            try
            {
                DateTime hostdate = ConvertUtil.LocalToUTC8(DateTime.Now);
                data = ConvertUtil.GetBsonDocumentByString(line, dmac);
                if (data == null) return null;
                dateStr = data.GetValue("time").AsInt64 + "";
                unixDate = long.Parse(dateStr);
                start = new DateTime(1970, 1, 1, 0, 0, 0);
                DateTime date = start.AddMilliseconds(unixDate);
                day_id = int.Parse(date.ToString("yyyyMMdd"));
                rowkey = ConvertUtil.getHbaseRowKeyUnique(date, dmac);

                data.Add(DAY_ID, day_id);
                data.Add(INDB_DATETIME, long.Parse(hostdate.ToString("yyyyMMddHHmmss")));
                string mac = data.GetValue("mac").AsString;
                string id = dmac + StringUtil.FormatMacString(mac) + dateStr.Replace("-", "").Replace(" ", "").Replace(":", "");
                data.Add(ConvertUtil.Unique_Key, id);

            } catch (Exception e)
            {
                Trace.TraceError("convert hit log line to json failed:"+line+"\r\n"+e.Message + "\r\n" + e.StackTrace);
                CommonUtil.LogException(dmac, e);
                return null;
            }

            data.Add(ROW_KEY, rowkey);
            return data;
        }

        public bool shouldInToDB(int count)
        {
            return count >= BATHCH;
        }
        public void BsonToDw(BsonDocument data, StringBuilder sb)
        {
            sb.Append(data.GetValue("dmac", "")).Append("\t").Append(data.GetValue("mac", "")).Append("\t")
                                           .Append(data.GetValue("ip", "")).Append("\t").Append(data.GetValue("time", "")).Append("\t")
                                           .Append(data.GetValue("hitID", "")).Append("\t").Append(data.GetValue("refHitID", "")).Append("\t")
                                           .Append(data.GetValue("uID", "")).Append("\t").Append(data.GetValue("posIdx", "")).Append("\t")
                                          .Append(data.GetValue("pageTime", "")).Append("\t").Append(data.GetValue("day_id", "")).Append("\t")
                                          .Append(DateTime.Now.ToString("yyMMddHHmmss")).AppendLine();
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
