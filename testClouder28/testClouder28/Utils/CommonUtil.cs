using Microsoft.Azure;
using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.IO;
using MongoDB.Driver;
using AlalyzeLog.Worker.Model;
using AlayzeLog.DBTools;
using analyzeLogWorkRole.Model;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Text.RegularExpressions;
using MongoDB.Bson.Serialization;

namespace AlalyzeLog.DBTools

{
    //public class CommonUtil
    //{
    //    

    //}
    public class CommonUtil
    {


        /** system log tar document contained files */
        private const string oldUvfile = "ip_count.log";
        private const string LOCAL_PV_LOG = "localhost.log";
        private const string SYSTEM_LOG = "car_system.log";
        private const string cdmaFlow = "cdma-flow";
        private const string lanDown = "ip_data_lan_down";// 下行网络流量
        private const string lanUp = "ip_data_lan_up";// 上行网络流量
        private const string netDown = "ip_data_net_down";// 下行3g流量
        private const string netUp = "ip_data_net_up";// 上行3g流量
        private const string INTERNET_FLOW_LOG = "3g-flow.log";// 3g浏览
        private const string PV_HTTP_LOG = "pv_http.log";// 
        private const string UV_TIME_LOG = "uv_time_flow.log";
        private const string _3gDropLog = "3g_drop_log";
        private const string squidAccessLog = "squid_access.log";
        /** GPS掉线日志 **/
        private const string GPS_DROP_LOG_FILE = "gps-drop.log";

      

        /** 开关机日志 **/
        private const string ON_OFF_LOG_FILE = "on-off.log";
        /** 内容升级状态日志 **/
        private const string CONTENT_UPDATE_LOG_FILE = "content_update.log";
        /** 设备信息日志 **/
        private const string DEVICE_INFO_FILE = "device.info";
        /** Wifi掉线日志 **/
        private const string WIFI_DROP_LOG_FILE = "wifi-drop.log";
        /** 电瓶电压日志 **/
        private const string VCC_QUALITY_LOG_FILE = "vcc-quality.log";

        /** hit.log日志 **/
        private const string HIT_LOG_FILE = "hit.log";
        private static Hit hitModel = new Hit();
        private static UV uvModel = new UV();
        private static PV pvModel = new PV();

       

        internal static void LogErrorMessage(string v, Exception e)
        {
            Console.Error.WriteLine(e.Message + "\r\n" + e.StackTrace);
        }
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

        private static BsonDocument GetBsonDocumentByString(string s, string dmac)
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
                Console.Error.WriteLine(dmac, s, ex);
                //       LogErrorMessage(dmac, s, ex);
            }
            return null;
        }



        public static DateTime LocalToUTC8(DateTime dateTime)
        {
            return TimeZoneInfo.ConvertTimeBySystemTimeZoneId(dateTime, TimeZoneInfo.Local.Id, "China Standard Time");
        }

        public static string GetCloudString(string key)
        {
            //     return CloudConfigurationManager.GetSetting(key);
            return null;
        }
        public static string GetSettingString(string key)
        {
            return null;
         //   return System.Configuration.ConfigurationManager.AppSettings[key];
        }

        public static string FormatMacAddress(string mac)
        {
            return mac.ToUpper().Replace("-", "").Replace("_", "").Replace(":", "").Replace("\n", "").Replace("\r", "");
        }



        /**
         * 处理sys压缩包解压日志文件
         * 
         * @param subDir
         * @param mac
         */
        public static void SingleLogDirToDB(string dmac, string fileName, StreamReader reader,string blobName)
        {
            if (reader.EndOfStream)
            {
                reader.DiscardBufferedData();
                reader.BaseStream.Seek(0, SeekOrigin.Begin);
                reader.BaseStream.Position = 0;
            }

            IHBaseModel handleModel = null;             
            using (reader)
            {
                try
                {
                    switch (fileName)
                    {
                        case oldUvfile:// 处理UV文件
                            //handleOldUvFile(dmac, reader);
                            break;
                        case LOCAL_PV_LOG:// 普列处理PV文件
                                          //handleOldPvFile(dmac, reader,blobName);
                            handleModel = pvModel;
                            break;
                        case PV_HTTP_LOG: // nginx access log 大巴日志 pv 日志
                         //   handlePvHttpLog(dmac, reader,blobName);
                            handleModel = pvModel;
                            break;
                        case HIT_LOG_FILE: //hit日志
                  //          handleHitLog(dmac, reader, blobName);
                            handleModel = hitModel;
                            break;
                        case UV_TIME_LOG: // uv 日志处理
                                          //           handleUvTimeLog(dmac, reader, blobName);
                            handleModel = uvModel;
                            break;

                        case SYSTEM_LOG:// 系统日志
                   //         handleSystemLog(dmac, reader);
                            break;
                        case cdmaFlow:// 3G流量日志
                    //        handleCdmaFlow(dmac, reader);
                            break;
                        case lanDown:// 下行网络流量
                            ////handleLanDown(dmac, reader);
                            break;
                        case lanUp: //上行网络流量
                            ////handleLanUp(dmac, reader);
                            break;
                        case netDown: // 3G下行流量
                            ////handleNetDown(dmac, reader);
                            break;
                        case netUp: // 3G上行流量
                            ////handleNetUp(dmac, reader);
                            break;
                        case INTERNET_FLOW_LOG:// 3G浏览日志
                     //       handle3gFlowLog(dmac, reader);
                            break;
                        case _3gDropLog: // 3g掉线
                       //     handle3gDropLog(dmac, reader);
                            break;
                        case squidAccessLog: // squid日志处理
                      //      handleSquidAccessLog(dmac, reader);
                            break;
                        case GPS_DROP_LOG_FILE: // GPS掉线日志
                            ////handleGpsDropLogFile(dmac, reader);
                            break;
                        case ON_OFF_LOG_FILE:
                            // 开关机日志
                            //// handleOnOffLogFile(dmac, reader);
                            break;
                        case CONTENT_UPDATE_LOG_FILE:
                            // 内容升级状态日志
                            ////handleContentUpdateLogFile(dmac, reader);
                            break;
                        case DEVICE_INFO_FILE: // 设备信息日志
                            ////handleDeviceInfoFile(dmac, reader);
                            break;
                        case WIFI_DROP_LOG_FILE: // Wifi掉线日志
                            ////handleWifiDropLogFile(dmac, reader);
                            break;
                        case VCC_QUALITY_LOG_FILE: //电池质量
                            ////handleVccQualityLogFile(dmac, reader);
                            break;
                       

                    }
                    if (handleModel != null) {
                        handleLog(dmac, reader, blobName, fileName, handleModel).Wait();
                    }
                    
                }
                catch (Exception ex)
                {
                    Trace.TraceError("解析文件异常"+blobName+"\\"+ fileName + ex.Message + "\r\n" + ex.StackTrace);
                    LogException(dmac, ex);
                }
            }
        }
        public static async Task handleLog(string dmac, StreamReader reader, string blobName,string fileName,IHBaseModel model) {
           List<BsonDocument> list = new List<BsonDocument>();
            
            string line = null;
            HashSet<string> ids = new HashSet<string>();
            int filelines = 0;
            int bu_count = 0;
            int bu_count_repeat = 0;

            while ((line = reader.ReadLine()) != null)
            {
                filelines++;
                BsonDocument data = model.LineToBson(line, dmac);

                try
                {
                    if (data != null) 
                    {
                        bu_count_repeat++;
                        if (ids.Add(data.GetValue(ConvertUtil.Unique_Key).ToString())) {
                            data.Remove(ConvertUtil.Unique_Key);
                            list.Add(data);
                        }        
                    }
                    if (model.shouldInToDB(list.Count)) {
                        List<BsonDocument> toHbase = list;
                        list = new List<BsonDocument>();
                        await  HBaseBLL.BatchInsertJsonAsync(toHbase, model);                  
                    }
                    
                }
                catch (Exception e)
                {
                    Trace.TraceError(e.Message+"\r\n"+e.StackTrace);
                    CommonUtil.LogException(dmac, e);
                }


            }
            if (list.Count > 0)
            {
                try
                {
                 await HBaseBLL.BatchInsertJsonAsync(list, model);
                }catch (Exception e)
                {
                    CommonUtil.LogException(dmac, e);
                }
                bu_count = ids.Count;
                ids.Clear();
                list.Clear();
          
            }


        }
     
      

        public static void LogException(string dmac, Exception ex)
        {
            Console.Error.WriteLine("dma " + dmac + ex.Message + "\r\n" + ex.StackTrace);
         
        }
        public static void LogException(string dmac,string myMessage, Exception ex)
        {
            Console.Error.WriteLine("dma " + dmac + ex.Message + "\r\n" + ex.StackTrace); Console.Error.WriteLine("dma " + dmac + ex.Message + "\r\n" + ex.StackTrace);

        }
      
    }


}