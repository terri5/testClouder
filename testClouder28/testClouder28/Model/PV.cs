using AlalyzeLog.DBTools;
using analyzeLogWorkRole.Model;
using MongoDB.Bson;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using testClouder28;
using System.Collections.Concurrent;
using System.Threading;
using System.Data;
using System.Data.SqlClient;

namespace AlalyzeLog.Worker.Model
{
    public class PV : IHBaseModel
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

        private readonly Dictionary<string, string> mobile_os_dict = new Dictionary<string, string>();
        private string os_str_regex = "";
        private readonly Dictionary<string, string> mobile_browser_dict = new Dictionary<string, string>();
        private string browser_str_regex = "";
        private readonly Dictionary<string, string> mobile_brand_dict = new Dictionary<string, string>();
        private string brand_str_regex = "";

        private ConcurrentQueue<string> dwFileQueue = new ConcurrentQueue<string>();

       

        private long cnt = 0;
        private long RCnt = 0; //去重前的记录数

        public PV()
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

        public CellSet.Row ToRowOfCellSet(BsonDocument pv)
        {
            CellSet.Row cellSetRow = new CellSet.Row() { key = Encoding.UTF8.GetBytes(pv.GetValue(ROW_KEY).AsString) };
            try
            {
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + DMAC.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(DMAC).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + MAC.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(MAC, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + IP.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(IP).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + DATETIME_POINT.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(DATETIME_POINT, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + CLIENT_AGENT.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(CLIENT_AGENT, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + HTTP_METHOD.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(HTTP_METHOD).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + HTTP_URI.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(HTTP_URI).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + HTTP_VERSION.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(HTTP_VERSION).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + HTTP_HOST.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(HTTP_HOST, "").AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + DAY_ID.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(DAY_ID).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + REFERER.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(REFERER).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + CLIENT_OS.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(CLIENT_OS).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + MOBILE_BRAND.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(MOBILE_BRAND).AsString) });
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + CLIENT_BROWSER.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(CLIENT_BROWSER).AsString)});
                cellSetRow.values.Add(new Cell { column = Encoding.UTF8.GetBytes(COLUMN_FAMILY + ":" + IN_DB_DATETIME.ToUpper()), data = Encoding.UTF8.GetBytes(pv.GetValue(IN_DB_DATETIME).AsInt64 + "") });

            }
            catch (Exception e)
            {
               Trace.TraceError(" pv covert to hbase model failed:" + pv + "\r\n" + e.Message + "\n" + e.StackTrace);
                CommonUtil.LogException(pv.GetValue(DMAC).AsString, e);
                return null;
            }

            return cellSetRow;
        }

        public string GetTableName()
        {
            return TABLE_NAME;
        }

        public string GetColumnFamily()
        {
            return COLUMN_FAMILY;
        }



        public BsonDocument LineToBson(string line, string dmac)
        {
            
            string ip = StringUtil.getSingleIpString(line);
            if (string.IsNullOrEmpty(ip))
            { //没有找到符合条件的IP地址
                return null;
            }
            string mac = "";
            if (ip.Contains("+"))
            {
                String[] ss = ip.Split(new char[] { '+' });
                ip = ss[0].Trim();
                mac = ss[1];
            }
            string dateStr = StringUtil.getDateTimeString(line);
            DateTime hostdate = ConvertUtil.LocalToUTC8(DateTime.Now);
            if (!string.IsNullOrEmpty(dateStr))
            {
                dateStr = dateStr.Substring(0, dateStr.Length - 6); //去掉文本行中的时区标示信息
                DateTime date = DateTime.ParseExact(dateStr, "dd/MMM/yyyy:HH:mm:ss", DateTimeFormatInfo.InvariantInfo);
                BsonDocument obj = new BsonDocument();
                dmac = StringUtil.FormatMacString(dmac).Replace("-", "").Replace(":", "");
                obj.Add("dmac", dmac);
                obj.Add("mac", mac);
                obj.Add("ip", ip);
                obj.Add(DATETIME_POINT, date.ToString("yyyy-MM-dd HH:mm:ss"));
                obj.Add("day_id", date.ToString("yyyyMMdd"));
                string[] strs = StringUtil.GetStringsByRegex(StringUtil.regexQuotes, line);
                bool blnUrl = false, blnRequest = false;
                string url = string.Empty, httpUri = string.Empty, httpMethod = string.Empty, httpVer = string.Empty,httpHost = string.Empty;
                foreach (string str in strs)
                {
                    if (Regex.IsMatch(str, StringUtil.RegexUrl))
                    {
                       
                        url = str;
                        blnUrl = true;
                    }
                    if (Regex.IsMatch(str, StringUtil.RexgexRequest))
                    {
                        string host = Regex.Match(str, StringUtil.RegexHttpHost).Value;
                        if (!Regex.IsMatch(host.Trim().ToUpper(), "^(GET|POST)"))
                        {
                            httpHost = host.Trim();
                        }

                        string m = Regex.Match(str, "(GET|POST|Get|Post|get|post)\\s{0,}").Value;
                        httpMethod = m.Trim();
                        string v = Regex.Match(str, "\\s{0,}(HTTP|http|Http)/\\d{0,}.\\d{0,}").Value;
                        httpVer = v.Trim();
                        httpUri = str.Replace(m, "").Replace(v, "");
                        blnRequest = true;
                    }

                }
                
                if (strs.Length > 2)
                {
                    obj.Add("clientAgent", strs[2]);
                    
                    obj.Add(CLIENT_OS, Regex.IsMatch(strs[2], os_str_regex) ? mobile_os_dict[Regex.Match(strs[2], os_str_regex).Value] : "");
                    obj.Add(MOBILE_BRAND, Regex.IsMatch(strs[2], brand_str_regex) ? mobile_brand_dict[Regex.Match(strs[2], brand_str_regex).Value] : "");
                    obj.Add(CLIENT_BROWSER, Regex.IsMatch(strs[2], browser_str_regex) ? mobile_browser_dict[Regex.Match(strs[2], browser_str_regex).Value] : "");
                  
                }

                //存在url的
                if (blnUrl || blnRequest)
                {
                    obj.Add(HTTP_URI, httpUri);
                    obj.Add(HTTP_METHOD, httpMethod);
                    obj.Add(HTTP_VERSION, httpVer);
                    obj.Add(HTTP_HOST, httpHost);
                    obj.Add(REFERER, url);
                    obj.Add(ConvertUtil.Unique_Key, dmac + date.ToString("yyyyMMddHHmmss").Substring(0, 11) + ip.Replace(".", "") + StringUtil.ConvertBase64(httpUri + url));
                    obj.Add(IN_DB_DATETIME, long.Parse(hostdate.ToString("yyyyMMddHHmmss")));
                    string rowkey = ConvertUtil.getHbaseRowKeyUnique(date, dmac);
                    obj.Add(ROW_KEY, rowkey);
                    return obj;
                }       
                return null;
            }
                return null;
             
          
        }

        public bool shouldInToDB(int count)
        {
            return count >= BATHCH;
        }

        public void BsonToDw(BsonDocument doc, StringBuilder sb)
        {

            sb.Append(doc.GetValue(DMAC, "")).Append("\t")
                                      .Append(doc.GetValue(MAC, "")).Append("\t")
                                      .Append(doc.GetValue(IP, "")).Append("\t")
                                      .Append(DateTime.Parse(doc.GetValue(DATETIME_POINT).AsString).ToString("yyyy-MM-dd HH:mm:ss")).Append("\t")
                                      .Append(StringUtil.GetTrimLengthString(doc.GetValue(CLIENT_AGENT).AsString, 8000)).Append("\t")
                                      .Append(doc.GetValue(HTTP_METHOD, "")).Append("\t")
                                      .Append(StringUtil.GetTrimLengthString(doc.GetValue(HTTP_URI).AsString, 8000)).Append("\t")
                                      .Append(doc.GetValue(HTTP_VERSION, "")).Append("\t")
                                      .Append(doc.GetValue(REFERER, "")).Append("\t")
                                      .Append(doc.GetValue(CLIENT_OS,"")).Append("\t")
                                      .Append(doc.GetValue(MOBILE_BRAND,"")).Append("\t")
                                      .Append(doc.GetValue(CLIENT_BROWSER,"")).Append("\t")
                                      .Append(doc.GetValue(DAY_ID, "")).Append("\t")
                                      .Append(doc.GetValue(IN_DB_DATETIME,"")).Append("\t")
                                      .Append(DateTime.Now.ToString("yyyyMMddHHmmss")).Append("\t")
                                      .Append(doc.GetValue(HTTP_HOST,"")).AppendLine();


        }

        public ConcurrentQueue<string> GetDwFileQueue()
        {
            return dwFileQueue;
        }
        public long GetCnt() {
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

        public void AddCnt(int cnt1)
        {
            Interlocked.Add(ref cnt,cnt1);
        }

        public void AddRCnt(int cnt1)
        {
            Interlocked.Add(ref RCnt,cnt1);
        }

        public void SetDataRow(DataTable dt, BsonDocument p)
        {
            throw new NotImplementedException();
        }

        public string getDwTable()
        {
            throw new NotImplementedException();
        }

        public void SetDataTableColumnsFromDB(DataTable dt, SqlConnection conn, string tabName)
        {
            throw new NotImplementedException();
        }
    }

  }
