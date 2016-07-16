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
using System.Text.RegularExpressions;

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
        public const string USER_AGENT = "userAgent";
        public const string CLIENT_OS = "client_os";
        public const string MOBILE_BRAND = "mobile_brand";
        public const string CLIENT_BROWSER = "client_browser";

        private readonly Dictionary<string, string> mobile_os_dict = new Dictionary<string, string>();
        private string os_str_regex = "";
        private readonly Dictionary<string, string> mobile_browser_dict = new Dictionary<string, string>();
        private string browser_str_regex = "";
        private readonly Dictionary<string, string> mobile_brand_dict = new Dictionary<string, string>();
        private string brand_str_regex = "";

        private long cnt = 0;
        private long RCnt = 0; //去重前的记录数
        public Hit()
        {
            initDirct();
        }
        private void initDirct()
        {//初始化字典
            mobile_os_dict.Add("Android", "Android");
            mobile_os_dict.Add("iPhone OS", "iPhone OS");
            mobile_os_dict.Add("Windows NT", "Windows NT");
            foreach (var os in mobile_os_dict.Keys)
            {
                os_str_regex += "(" + os + ")" + "|";
            }
            os_str_regex = os_str_regex.Substring(0, os_str_regex.Length - 1);

            mobile_browser_dict.Add("QQBrowser", "QQBrowser");
            mobile_browser_dict.Add("Firefox", "Firefox");
            mobile_browser_dict.Add("Chrome", "Chrome");
            mobile_browser_dict.Add("Safari", "Safari");
            mobile_browser_dict.Add("Opera", "Opera");
            mobile_browser_dict.Add("UCBrowser", "UCBrowser");
            mobile_browser_dict.Add("360Browser", "360Browser");

            foreach (var browser in mobile_browser_dict.Keys)
            {
                browser_str_regex += "(" + browser + ")" + "|";
            }
            browser_str_regex = browser_str_regex.Substring(0, browser_str_regex.Length - 1);

            mobile_brand_dict.Add("iPhone", "iPhone");
            mobile_brand_dict.Add("HUAWEI", "HUAWEI");
            mobile_brand_dict.Add("MIUI", "MIUI");
            mobile_brand_dict.Add("SAMSUNG", "SAMSUNG");
            mobile_brand_dict.Add("OPPO", "OPPO");
            mobile_brand_dict.Add("MEIZU", "MEIZU");
            mobile_brand_dict.Add("VIVO", "VIVO");
            mobile_brand_dict.Add("Coolpad", "Coolpad");
            mobile_brand_dict.Add("HTC", "HTC");
            mobile_brand_dict.Add("ZTE", "ZTE");
            mobile_brand_dict.Add("Lenovo", "Lenovo");
            mobile_brand_dict.Add("sony", "sony");

            foreach (var brand in mobile_brand_dict.Keys)
            {
                brand_str_regex += "(" + brand + ")" + "|";
            }
            brand_str_regex = brand_str_regex.Substring(0, brand_str_regex.Length - 1);
        }


        public CellSet.Row ToRowOfCellSet(BsonDocument hit)
        {
            CellSet.Row cellSetRow = new CellSet.Row() { key = Encoding.UTF8.GetBytes(hit.GetValue(ROW_KEY).AsString) };
            try
            {
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + DMAC.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(DMAC).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + MAC.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(MAC, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + IP.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(IP).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + TIME.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(TIME).AsInt64 + "") });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + HITID.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(HITID).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + REFHITID.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(REFHITID, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + UID.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(UID, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + POSIDX.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(POSIDX, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + PAGETIME.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(PAGETIME, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + DAY_ID.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(DAY_ID).AsInt32 + "") });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + CLIENT_OS.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(CLIENT_OS,"").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + MOBILE_BRAND.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(MOBILE_BRAND,"").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + CLIENT_BROWSER.ToUpper()), data = Encoding.UTF8.GetBytes(hit.GetValue(CLIENT_BROWSER,"").AsString) });
             
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

        public BsonDocument LineToBson(string line, string dmac)
        {
            string dateStr;
            int day_id = 0;

            long unixDate;
            DateTime start;

            //主机时间
            BsonDocument data = null;
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
                if (data.Contains(USER_AGENT))
                {
                    string str = data.GetValue(USER_AGENT).AsString;
                    data.Add(CLIENT_OS, Regex.IsMatch(str, os_str_regex) ? mobile_os_dict[Regex.Match(str, os_str_regex).Value] : "");
                    data.Add(MOBILE_BRAND, Regex.IsMatch(str, brand_str_regex) ? mobile_brand_dict[Regex.Match(str, brand_str_regex).Value] : "");
                    data.Add(CLIENT_BROWSER, Regex.IsMatch(str, browser_str_regex) ? mobile_browser_dict[Regex.Match(str, browser_str_regex).Value] : "");
                }

            }
            catch (Exception e)
            {
                Trace.TraceError("convert hit log line to json failed:" + line + "\r\n" + e.Message + "\r\n" + e.StackTrace);
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
                                          .Append(data.GetValue(INDB_DATETIME,"")).Append("\t")
                                          .Append(DateTime.Now.ToString("yyyyMMddHHmmss")).Append("\t")
                                          .Append(data.GetValue(CLIENT_OS, "")).Append("\t")
                                          .Append(data.GetValue(MOBILE_BRAND, "")).Append("\t")
                                          .Append(data.GetValue(CLIENT_BROWSER, "")).AppendLine();
                                          
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
