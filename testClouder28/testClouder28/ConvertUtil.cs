using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace WifiBox.DBTools
{


   public  class ConvertUtil
    {
        //mongoserver字典
        private static Dictionary<string, MongoServer> DictionaryMongoServers = new Dictionary<string, MongoServer>();
        private static string[] MongoServerKeys = { "MongoServer1", "MongoServer2" };
        static ConvertUtil()
        {
           
            foreach (string mKey in MongoServerKeys)
            {
                string conn = CommonUtil.GetSettingString(mKey);
                MongoClient client = new MongoClient(conn);
                DictionaryMongoServers.Add(mKey, client.GetServer());
            }
            
        }
        public static string LongDateStringToDayString(string lnDateStr)
        {
            return Convert.ToDateTime(lnDateStr).ToString("yyyyMMdd");
        }

        public static MongoCollection GetMongoCollectionByName(string key)
        {
            return GetMongoCollectionByName(key, DateTime.Now);
        }

        public static MongoCollection GetMongoCollectionByName(string setKey, DateTime date)
        {
            string appValue = CommonUtil.GetSettingString(setKey);
            string[] ss = appValue.Split('/');
            if (ss.Length == 3)
            {
                string mKey = ss[0].Trim();
                string dbName = ConvertMongoName(ss[1].Trim(), date);
                string colName = ConvertMongoName(ss[2].Trim(), date);
                //判断关键字对应的mongoserver是否存在
                if (DictionaryMongoServers.ContainsKey(mKey))
                {
                    return  DictionaryMongoServers[mKey].GetDatabase(dbName).GetCollection(colName);
                }
            }
            return null;
        }

        /// <summary>
        /// 转换mongo的数据库名和集合名称
        /// </summary>
        /// <param name="key"></param>
        /// <param name="date"></param>
        /// <returns></returns>
        public static string ConvertMongoName(string key, DateTime date)
        {
            string reg = "\\[[A-Za-z]+\\]";
            if (Regex.IsMatch(key, reg))
            {
                string source = Regex.Match(key, reg).Value;
                key = key.Replace(source, date.ToString(Regex.Match(source, "[A-Za-z]+").Value));
            }
            return key;
        }

        public static String reverseStr(string str) {
            if (str == null) return null;
            return new string(str.ToArray().Reverse().ToArray());
        }

        public static DateTime LocalToUTC8(DateTime dateTime)
        {
            return TimeZoneInfo.ConvertTimeBySystemTimeZoneId(dateTime, TimeZoneInfo.Local.Id, "China Standard Time");
        }

        public static string getHbaseRowKeyUnique(DateTime dateTime) {
            Guid tempCartId = Guid.NewGuid();
            return ConvertUtil.reverseStr(ToTimestamp(dateTime)+"")+ tempCartId.ToString().Substring(0, 6).ToUpper();
        }

        public static long ToTimestamp(DateTime value)
        {
            TimeSpan span = (value - new DateTime(1970, 1, 1, 0, 0, 0, 0).ToLocalTime());
            return (long)span.TotalMilliseconds;
        }


    }
}
