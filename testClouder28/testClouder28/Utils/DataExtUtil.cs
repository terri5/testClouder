using MongoDB.Driver;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace AlalyzeLog.DBTools
{
    public class DataExtUtil
    {


        public static void BatchCopyDataToSqlDw(int step, DataTable dt, SqlConnection conn, String tabName)
        {

            Stopwatch watch = new Stopwatch();
            watch.Start();
            using (var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null))
            {
                try
                {
                    bulk.DestinationTableName = tabName;
                    bulk.BatchSize = step;
                    bulk.BulkCopyTimeout = 6000;
                    bulk.WriteToServer(dt);
                    Console.WriteLine("SqlBulkCopy WriteToServer {0} rows", dt.Rows.Count);
                    dt.Rows.Clear();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    Console.WriteLine(ex.StackTrace);
                    Console.ReadKey();
                }
                watch.Stop();
                Console.WriteLine("Time expend {0} seconds", watch.ElapsedMilliseconds / 1000);
                GC.Collect();
            }

        }

      
        private static Random r = new Random();

        public static int Convet16To10(string input)
        {
            if (!string.IsNullOrEmpty(input))
            {
                try
                {
                    return Convert.ToInt32(input.Substring(input.Length - 2, 2), 16);
                }
                catch
                {
                    Trace.WriteLine(string.Format("error, don't convert mac:[{0}] to decimal system!", input));
                }
            }
            return r.Next(0, 255);
        }

        public static string GetTrimLengthString(string input, int len)
        {
            return string.IsNullOrEmpty(input) ? string.Empty : input.Length <= len ? input : input.Substring(0, len);
        }

        /*
         * 
         * 解决诸如  "yyyy/MM/dd HH:mm:ss","yyyy/M/d HH:mm:ss","yyyy/MM/d HHmmss","yyyy/M/dd HH:mm:ss",
         *            "yyyy/M/dd H:mm:ss" 之类的日期问题
         */
        public static string getNormalDateString(string str)
        {
            if (str == null)
            {
                return "";
            }
            String strYmd = "";
            string strHms = "";
            if (str.Contains("/"))
            {

                string[] strd = str.Split(' ');
                strYmd = strd[0].Replace('/', '-');
                if (strd[0].Length < 10)
                {
                    //  Console.WriteLine(strd[0]);
                    string[] strmd = strd[0].Split('/');
                    if (strmd[1].Length < 2) strmd[1] = "0" + strmd[1];
                    if (strmd[2].Length < 2) strmd[2] = "0" + strmd[2];

                    strYmd = strmd[0] + "-" + strmd[1] + "-" + strmd[2];

                }
                strHms = strd[1];
                if (strd[1].Length < 8)
                {
                    string[] strHmsArr = strd[1].Split(':');

                    //          Console.WriteLine(strHmsArr[0].Length + " index="+ strd[1].IndexOf(":"));
                    if (strHmsArr[0].Length < 2) strHmsArr[0] = "0" + strHmsArr[0];
                    if (strHmsArr[1].Length < 2) strHmsArr[1] = "0" + strHmsArr[1];
                    if (strHmsArr[2].Length < 2) strHmsArr[2] = "0" + strHmsArr[2];
                    strHms = strHmsArr[0] + ":" + strHmsArr[1] + ":" + strHmsArr[2];
                }
                return strYmd + " " + strHms;

            }
            else
            {
                return str;

            }


        }

        public static bool IsNumeric(string value)
        {
            return Regex.IsMatch(value, @"^-?/d*[.]?/d*$");
        }
        public static bool IsInt(string value)
        {
            return Regex.IsMatch(value, @"^-?\d+$");
        }
        public static bool IsUnsign(string value)
        {
            return Regex.IsMatch(value, @"^/d*[.]?/d*$");
        }

        public static string DateTimeStrAddDay(string dayId, int addDays)
        {
            DateTime endDay = DateTime.ParseExact(dayId + "", "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
            endDay = endDay.AddDays(addDays);
            string endDayId = endDay.ToString("yyyyMMdd");
            return endDayId;
        }

        public static double getDoubleFormObj(object o, int len = 3)
        {
            try
            {
                double d = Convert.ToDouble(o.ToString());
                string str = "";
                if (len == 6)
                {
                    str = String.Format("{0:#0.000000}", d);
                }
                else
                {
                    str = String.Format("{0:#0.000}", d);
                }

                return Convert.ToDouble(str);
            }
            catch (Exception e)
            {
                throw new ApplicationException("数值格式异常:" + o);
            }
        }

    }
}