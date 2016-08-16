using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AlalyzeLog.DBTools
{
    class StringUtil
    {
        // ip地址正则表达式
        private const string REGEXIP = @"192.168.(25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))).((25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))))";
        private const string REGEXIP_EX = @"192.168.(25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))).((25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))))(\s|\+[0-9a-zA-Z]{2}:[0-9a-zA-Z]{2}:[0-9a-zA-Z]{2}:[0-9a-zA-Z]{2}:[0-9a-zA-Z]{2}:[0-9a-zA-Z]{2})";
        private const string IS_RIGHTFUL_IP = "^((\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5]|[*])\\.){3}(\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5]|[*])$";
        // 日期正则表达式
        private const string regexDate = "(19|20)\\d{2}[/\\s\\-\\.]*(0[1-9]|1[0-2]|[1-9])[/\\s\\-\\.]*(0[1-9]|3[01]|[12][0-9]|[1-9])(.|\\-|\\s)*(2[0-3]|[01]?\\d)(:[0-5]\\d){0,2}";
        // 西文日期格式正则表达式
        private const string RegexEnDate = @"\d{2}/[a-zA-Z]{3}/\d{4}:\d{2}:\d{2}:\d{2}\s{1}\+\d{4}";

        // 引号之间
        public const string regexQuotes = "\"(.*?)\"";
        // URL
        public const string RegexUrl = "((http|ftp|https)://)(([a-zA-Z0-9\\._-]+\\.[a-zA-Z]{2,6})|([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\\&%_\\./-~-]*)?";
        //Reuqest 
        public const string RexgexRequest = "(GET|POST|Get|Post|get|post)\\s{0,}\\S{2,}\\s{0,}(HTTP|http|Http)/\\d{0,}.\\d{0,}";

        public const string RegexHttpHost = "(([a-zA-Z0-9_-])+(\\.)?)*(:\\d+)?";

        // ip流量
        private const string regexIpFlow = "\"192.168.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5]).(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\"\\:\\s\\d+(\\.\\d+)?";
        private const string regexIpMacFlow = "{\"ip\":.*?\"mac\":.*?\"flow\":.*?}";


        public static string GetTrimLengthString(string input, int len)
        {
            return string.IsNullOrEmpty(input) ? string.Empty : input.Length <= len ? input : input.Substring(0, len);
        }

        public static string getSquidAccessLog(string line, string dmac)
        {
            try
            {
                string[] strs = line.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                List<string> al = new List<string>();
                foreach (string s in strs)
                {
                    if (!(string.IsNullOrEmpty(s) || s.Equals("-")))
                    {
                        al.Add(s);
                    }
                }
                //String[] ss = (String[]) al.ToArray(new String[al.size()]);
                DateTime date = new DateTime(long.Parse(al[0].Replace(".", "").Trim()));
                int requestTime = int.Parse(al[1].Trim());
                string ip = al[2].Trim();
                string result = al[3].Trim();
                int transSize = int.Parse(al[4].Trim());
                string requestsWay = al[5].Trim();
                string url = al[6].Trim();
                string direct = al[7].Trim();
                string mime = "";
                if (al.Count > 8)
                {
                    mime = al[8].Trim();
                }
                BsonDocument obj = new BsonDocument();
                string curId = dmac + al[0] + ip;
                obj.Add("_id", curId.Replace(".", ""));
                obj.Add("dmac", dmac);
                obj.Add("day_id", date.ToString("yyyyMMdd"));
                obj.Add("date", date.ToString("yyyy-MM-dd HH:mm:ss"));
                obj.Add("request_time", requestTime);
                obj.Add("ip", ip);
                obj.Add("result", result);
                obj.Add("trans_size", transSize);
                obj.Add("requests_way", requestsWay);
                obj.Add("url", url);
                obj.Add("direct", direct);
                obj.Add("mime", mime);
                return obj.ToJson();
            }
            catch (Exception e)
            {
               CommonUtil.LogException(dmac, e);
            }
            return null;
        }

        private static Random r = new Random();
        public static BsonDocument GetSimPV(string dayId, string dmac)
        {

            if (r.Next(10) > 5)
            {
                BsonDocument obj = new BsonDocument();
                string httpUrl = "/bootstrap.html";
                obj.Add("httpUri", httpUrl);
                obj.Add("httpMethod", "GET");
                obj.Add("httpVer", "HTTP/1.1");
                obj.Add("url", "-");
                string ip = "192.168.17." + r.Next(200);
                obj.Add("_id", dmac + dayId + ip.Replace(".", "") + ConvertBase64(httpUrl));
                string mac = string.Format("5c:f{0}:{1}a:c{2}:{3}e:c{4}", r.Next(9), r.Next(9), r.Next(9), r.Next(9), r.Next(9));
                obj.Add("dmac", dmac);
                obj.Add("mac", mac);
                obj.Add("ip", ip);
                obj.Add("date", string.Format("2016-04-24 {0}{1}:{2}{3}:{4}{5}", r.Next(1), r.Next(9), r.Next(5), r.Next(9), r.Next(5), r.Next(9)));
                obj.Add("day_id", dayId);
                return obj;
            }
            return null;
        }

        public static string ConvertBase64(string str)
        {
            str = str.Replace("/", "").Replace(" ", "").Replace(".", "");
            str = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(str));
            if (str.Length > 916)
            {
                str = str.Substring(0, 916);
            }
            return str;
        }

        public static string FormatDateString(string dateStr)
        {
            if (string.IsNullOrEmpty(dateStr))
                return null;
            return dateStr.Replace(":", "").Replace("-", "").Replace(" ", "");
        }

        public static string encodeByMD5(string str)
        {
            if (string.IsNullOrEmpty(str))
            {
                return null;
            }
            MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider();

            byte[] bytValue, bytHash;

            bytValue = System.Text.Encoding.UTF8.GetBytes(str);

            bytHash = md5.ComputeHash(bytValue);

            md5.Clear();

            string sTemp = "";

            for (int i = 0; i < bytHash.Length; i++)
            {

                sTemp += bytHash[i].ToString("X").PadLeft(2, '0');

            }

            return sTemp.ToUpper();
        }
        /// <summary>
        /// 得到引号中的数据
        /// </summary>
        /// <param name="regexStr"></param>
        /// <param name="s"></param>
        /// <returns></returns>
        public static string[] GetStringsByRegex(string regexStr, string s)
        {
            List<string> list = new List<string>();
            foreach (Match m in Regex.Matches(s, regexStr))
            {
                list.Add(m.Value.Replace("\"", ""));
            }
            return list.ToArray();
        }




        public static string getSingleIpString(string s)
        {
            if (Regex.IsMatch(s, REGEXIP_EX))
            {
                return Regex.Match(s, REGEXIP_EX).Value;
            }
            return null;
        }


        // 通过正则得到字符串中的日期值
        public static string getDateTimeString(string s)
        {

            if (Regex.IsMatch(s, RegexEnDate))
            {
                return Regex.Match(s, RegexEnDate).Value;
            }
            return null;
        }




        public static string FormatMacString(string mac)
        {
                      return CommonUtil.FormatMacAddress(mac);
            
        }

    }
}
