using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AlalyzeLog.DBTools
{


   public  class ConvertUtil
    {
        public const string Unique_Key = "_id";
        //mongoserver字典
      
        static ConvertUtil()
        {
                      
        }
        public static string LongDateStringToDayString(string lnDateStr)
        {
            return Convert.ToDateTime(lnDateStr).ToString("yyyyMMdd");
        }

        public static BsonDocument GetBsonDocumentByString(string s, string dmac)
        {
            try
            {
                string regexDate = "\"date\"\\s{0,}:\\s{0,}\"(\\d{4}-\\d{2}-\\d{2}-\\d{2}:\\d{2}:\\d{2}|\\s{0,})\"\\s{0,},";
                if (Regex.IsMatch(s, regexDate))
                {
                    MatchCollection mc = Regex.Matches(s, regexDate);
                    //如果有两个以上的date
                    if (mc.Count > 1)
                    {
                        for (int i = 0; i < mc.Count; i++)
                        {
                            if (i > 0)
                            {
                                s = s.Replace(mc[i].Value, "");
                            }
                        }
                    }
                }

                BsonDocument doc = BsonSerializer.Deserialize<BsonDocument>(s);
                if (!doc.Contains("dmac"))
                {
                    doc.Add("dmac", dmac);
                }
                return doc;
            }
            catch (Exception ex)
            {
             //   LogErrorMessage(dmac, s, ex);
            }
            return null;
        }

     

       
        public static String reverseStr(string str) {
            if (str == null) return null;
            return new string(str.ToArray().Reverse().ToArray());
        }

        public static DateTime LocalToUTC8(DateTime dateTime)
        {
            return TimeZoneInfo.ConvertTimeBySystemTimeZoneId(dateTime, TimeZoneInfo.Local.Id, "China Standard Time");
        }
       /**
        * 
        */
        public static string getHbaseRowKeyUnique(DateTime businessTime,string dmac) {
            Guid tempCartId = Guid.NewGuid();
            Random rand = new Random();
            DateTime now = LocalToUTC8(DateTime.Now);
            return now.ToString("yyyyMMdd")+rand.Next(10)+now.ToString("HHmm")+ dmac+ businessTime.ToString("yyyyMMddHHmm") + tempCartId.ToString().Substring(0, 6).ToUpper();
        }

        /**
     * 调整时间格式中的缺失
     */
        public static DateTime TrimDateMissing(BsonValue dateTimeObject)
        {
            // 使用正则过滤
            DateTime date = DateTime.Now;
            if (dateTimeObject == null)
            {
                throw new Exception("当前时间对象为空");
            }
            string dateStr = dateTimeObject.AsString;
            string reg1 = "\\d{4}-\\d{2}-\\d{2}\\s{1}\\d{2}:\\d{2}:\\d{2}";//yyyy-MM-dd HH:mm:ss
            if (Regex.IsMatch(dateStr, reg1))
            {
                date = DateTime.ParseExact(dateStr, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.CurrentCulture);
            }
            string reg2 = "\\d{2}-\\d{2}-\\d{2}\\s{1}\\d{2}:\\d{2}:\\d{2}";//yy-MM-dd HH:mm:ss
            if (Regex.IsMatch(dateStr, reg2))
            {
                date = DateTime.ParseExact(dateStr, "yy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.CurrentCulture);
            }
            string reg3 = "\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}";//yyyy-MM-dd-HH-mm-ss
            if (Regex.IsMatch(dateStr, reg3))
            {
                date = DateTime.ParseExact(dateStr, "yyyy-MM-dd-HH-mm-ss", System.Globalization.CultureInfo.CurrentCulture);
            }
            string reg4 = "\\d{4}-\\d{2}-\\d{2}\\d{2}-\\d{2}-\\d{2}";//yyyy-MM-ddHH-mm-ss
            if (Regex.IsMatch(dateStr, reg4))
            {
                date = DateTime.ParseExact(dateStr, "yyyy-MM-ddHH-mm-ss", System.Globalization.CultureInfo.CurrentCulture);
            }
            string reg5 = "\\d{4}-\\d{2}-\\d{2}-\\d{2}:\\d{2}:\\d{2}";//yyyy-MM-dd-HH:mm:ss
            if (Regex.IsMatch(dateStr, reg5))
            {
                date = DateTime.ParseExact(dateStr, "yyyy-MM-dd-HH:mm:ss", System.Globalization.CultureInfo.CurrentCulture);
            }
            string reg6 = "\\d{4}\\d{2}\\d{2}";////yyyyMMdd
            if (Regex.IsMatch(dateStr, reg6))
            {
                date = DateTime.ParseExact(dateStr, "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
            }
            string reg7 = "\\d{2}/\\d{2}/\\d{4}:\\d{2}:\\d{2}:\\d{2}";//dd/MM/yyyy:HH:mm:ss
            if (Regex.IsMatch(dateStr, reg7))
            {
                date = DateTime.ParseExact(dateStr, "dd/MM/yyyy:HH:mm:ss", System.Globalization.CultureInfo.CurrentCulture);
            }
            return date;
        }


        public static long ToTimestamp(DateTime value)
        {
            TimeSpan span = (value - new DateTime(1970, 1, 1, 0, 0, 0, 0).ToLocalTime());
            return (long)span.TotalMilliseconds;
        }

        public static void SafePutBsonValue(BsonDocument data, string key, object value)
        {
            if (data.Contains(key))
            {
                data.Remove(key);
            }
            data.Add(key, BsonValue.Create(value));
        }

    }
}
