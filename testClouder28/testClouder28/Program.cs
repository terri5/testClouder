using AlalyzeLog.DBTools;
using AlalyzeLog.Worker.Model;
using AlayzeLog.DBTools;
using analyzeLogWorkRole.Model;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Tar;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using RedisStudy;
using ServiceStack.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;



namespace testClouder28
{
    class
        Program
    {

        static string storageConnectionString = "BlobEndpoint=https://cloudstoresys.blob.core.chinacloudapi.cn/;AccountName=cloudstoresys;AccountKey=SGD3zOxPKaPxal06CYZOPm6SwBMUHIv6e+wnGwY67rMee+pEIZWjh0g3bEI1BiKZuPHeSL/Q+PdGpnN2Etdftw==";

        //Reuqest 
        private const string RexgexRequest = "(GET|POST|Get|Post|get|post)\\s{0,}\\S{2,}\\s{0,}(HTTP|http|Http)/\\d{0,}.\\d{0,}";
        private const string REGEXIP_EX = @"192.168.(25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))).((25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))))(\s|\+[0-9a-zA-Z]{2}:[0-9a-zA-Z]{2}:[0-9a-zA-Z]{2}:[0-9a-zA-Z]{2}:[0-9a-zA-Z]{2}:[0-9a-zA-Z]{2})";
        // 西文日期格式正则表达式
        private const string RegexEnDate = @"\d{2}/[a-zA-Z]{3}/\d{4}:\d{2}:\d{2}:\d{2}\s{1}\+\d{4}";

        // URL
        private const string RegexUrl = "((http|ftp|https)://)(([a-zA-Z0-9\\._-]+\\.[a-zA-Z]{2,6})|([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\\&%_\\./-~-]*)?";

        // 引号之间
        private const string regexQuotes = "\"(.*?)\"";


        private static Object syncUvF = new object();
        private static Object syncHitF = new object();
        private static Object syncPvF = new object();
        private static Object syncPv2F = new object();
        private static Object syncFileCount = new object();

        public const string cnstr = @"Server=tcp:amsqldwserver2016.database.chinacloudapi.cn,1433;Database=amwifiboxsqldw;User ID=amsqldwsa@amsqldwserver2016;Password=Password!23;Trusted_Connection=False;Encrypt=True;Connection Timeout=300;";


        public static System.Collections.Generic.HashSet<string> file2Handle = new System.Collections.Generic.HashSet<string>();

        public static string date2Handle = "20160905";
        public const int step = 100000;
        public const long G = 1024 * 1024 * 1024;
        public const long MAX_CACHE= 20 * G;
        public static long current_mem_que_size = 0;

        private static Object que_size_lock = new Object();
        


        public const int AnlyzeThreadCnt =32;//总解析线程数
        public const int Write2HbaseThreadCnt = 32;//总写hbase线程数

        private static StreamWriter uv2f = null;
        private static StreamWriter pv2f = null;
        private static StreamWriter pv22f = null;
        private static StreamWriter hit2f = null;
        public static StreamWriter log = null;
        public static FileStream log_err = null;
        public static HashSet<string> downloadDir = new HashSet<string>();

        public static string driver = "e:";
        public static ConcurrentQueue<Stream> memQueue = new ConcurrentQueue<Stream>();
        /**
        * 整理后的文件目录
        */
        public static string correct_dir = driver + @"\cor" + date2Handle;

        /**
         * blob文件目录
         */
        public static string orgin_dir = driver + "\\" + date2Handle;


        private static string dataDirRoot = driver + @"\test-data\";
        public static HashSet<string> hit_dmac = new HashSet<string>();
        
        private static void initFolders()
        {
            string dataDir =dataDirRoot + date2Handle;
            if (!Directory.Exists(dataDir)) {
                Directory.CreateDirectory(dataDir);//创建新路径
            }
            uv2f = new StreamWriter(new FileStream( dataDir+ "\\uv.txt", FileMode.Append), Encoding.GetEncoding("gb2312"));
            pv2f = new StreamWriter(new FileStream(dataDir + "\\pv.txt", FileMode.Append), Encoding.GetEncoding("gb2312"));
            hit2f = new StreamWriter(new FileStream(dataDir + "\\hit.txt", FileMode.Append), Encoding.GetEncoding("gb2312"));
            pv22f = new StreamWriter(new FileStream(dataDir + "\\proxy_pv.txt", FileMode.Append), Encoding.GetEncoding("gb2312"));
        }
    
        private static void initLogFolders() {
            if (!Directory.Exists(orgin_dir ))
            {
                Console.WriteLine(orgin_dir);
                Directory.CreateDirectory(orgin_dir);//创建新路径
            }

            if (!Directory.Exists(correct_dir)){
                Console.WriteLine(correct_dir);
                Directory.CreateDirectory(correct_dir);//创建新路径
            }
            if (!Directory.Exists(dataDirRoot))
            {
                Directory.CreateDirectory(dataDirRoot);//创建新路径
            }

            log = new StreamWriter(new FileStream(dataDirRoot + "exe-" + DateTime.Now.ToString("yyyy-MM-dd-HH-mm") + ".log", FileMode.Append), Encoding.GetEncoding("UTF-8"));
            log_err = new FileStream(dataDirRoot + "err-" + DateTime.Now.ToString("yyyy-MM-dd-HH-mm") + ".log", FileMode.Append);
        }

        public static System.Collections.Concurrent.ConcurrentQueue<LogFileInfo> logfiles = new ConcurrentQueue<LogFileInfo>();



        public static Hit hitModel = new Hit();
        public static UV uvModel = new UV();
        public static PV pvModel = new PV();
        public static PV2 pv2Model = new PV2();

        public static ConcurrentQueue<string> pvDwFileQueue = pvModel.GetDwFileQueue();
        public static ConcurrentQueue<string> pv2DwFileQueue = pv2Model.GetDwFileQueue();

        public static ConcurrentQueue<string> uvDwFileQueue = uvModel.GetDwFileQueue();
        public static ConcurrentQueue<string> hitDwFileQueue = hitModel.GetDwFileQueue();

        public static ConcurrentQueue<BsonDocument> pvHbaseQueue = new ConcurrentQueue<BsonDocument>();
        public static ConcurrentQueue<BsonDocument> hitDwQueue = new ConcurrentQueue<BsonDocument>();


        public static string validTarStr = date2Handle.Substring(2, 6) + "\\d{6}\\S{7}$";
        public static string blobContainer = date2Handle.Substring(0,8);
        public static int handleHitFileCnt = 0;
        public static int handlePvFileCnt = 0;
        public static int handlePv2FileCnt = 0;
        public static int handleUvFileCnt = 0;
        public static Boolean addCompleted = false;
        public static Boolean anlyzeCompleted = false;
        public static Boolean allCompleted = false;
        public static int handleFileCnt = 0;
        public static System.Collections.Generic.HashSet<string> uv_id = new System.Collections.Generic.HashSet<string>();


        public static Task taskPv = null;
        public static Task taskUv = null;
        public static Task taskHit = null;
  

        static void Main(string[] args)
        {
            try
            {
                Stopwatch watch = new Stopwatch();
                watch.Start();//首先启动监控线程
                initArgs(args);
                initLogFolders();//初始化相关目录

                log.WriteLine(date2Handle + "开始时间{0},处理日期{1}", DateTime.Now, date2Handle);
                Console.WriteLine(date2Handle + "开始时间{0},处理日期{1}", DateTime.Now, date2Handle);

                Thread monitor = new Thread(PipelineStages.MonitorThread);//首先启动监控线程
                monitor.Start(logfiles);



                // testHbaseWrite(20160817+"");
                 // readFile2Dw(date2Handle);
                
                anlylogFromBlob(blobContainer);
                anlylog();



                Console.WriteLine("处理文档数量{0},pv:{1},pv2:{2},uv:{3},hit:{4}", handleFileCnt, handlePv2FileCnt, handlePvFileCnt, handleUvFileCnt, handleHitFileCnt);
                Console.WriteLine("处理有效记录数量 pv:{0},pv2:{1},uv:{2},hit:{3}", pvModel.GetCnt(),pv2Model.GetCnt(),uvModel.GetCnt(), hitModel.GetCnt());

                watch.Stop();
                TimeSpan timeSpan = watch.Elapsed;
                Console.WriteLine("Time expend {0} seconds", watch.ElapsedMilliseconds / 1000);
                Console.WriteLine("耗费{0}天{1}小时{2}分钟{3}秒", timeSpan.Days, timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);
                log.WriteLine("处理文档数量{0},pv:{1},uv:{2},hit:{3}", handleFileCnt, handlePvFileCnt, handleUvFileCnt, handleHitFileCnt);
                log.WriteLine("处理有效记录数量 pv:{0},uv:{1},hit:{2}", pvModel.GetCnt(), uvModel.GetCnt(), hitModel.GetCnt());
                log.WriteLine("耗费{0}天{1}小时{2}分钟{3}秒", timeSpan.Days, timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);
               // Console.ReadKey();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message + "\r\n" + e.StackTrace);
                WriteLog(e);
                Console.ReadKey();
            }
            finally{

                if (log_err != null)
                {
                    log_err.Flush();
                    log_err.Close();
                }
                if (log != null) {
                    log.Flush();
                    log.Close();
                }
             
            }
           


        }

        private static void initArgs(string[] args)
        {
         
            if (args.Length > 0) {
                if (DataExtUtil.IsInt(args[0]) && args[0].Length!=8 && args[0].Length != 10) {
                    Console.Error.WriteLine("非法的参数 day_id or day_hour:"+args[0]);
                    Console.ReadKey();
                    System.Environment.Exit(-1);
                };
                
                date2Handle = args[0];

                if(args[0].Length == 10) validTarStr = date2Handle.Substring(2,8) + "\\d{4}\\S{7}$";
                if(args[0].Length==8) validTarStr = date2Handle.Substring(2, 6) + "\\d{6}\\S{7}$";
                blobContainer = date2Handle.Substring(0, 8);

                if (args.Length > 1)
                    if (args[1].Length == 1)
                    {
                        driver = args[1] + ":";
                    } else {
                        Console.Error.WriteLine("非法的参数 driver:" + args[0]);
                        Console.ReadKey();
                        System.Environment.Exit(-1);
                    }
              correct_dir = driver + @"\cor" + date2Handle;
              orgin_dir = driver + "\\" + date2Handle;
//                Console.WriteLine("regex: {0}", validTarStr);
//                Console.WriteLine("参数个数是：{0} 值是{1}", args.Length, args[0]);

            }
            Console.WriteLine("blobContainer:" + blobContainer);
        }

        public static void anlylogFromBlob(string day_id) {
              DownloadBlob(day_id);
              log.WriteLine("{0} 共下载 目录{1} 个", day_id, downloadDir.Count);
            //listBlob();

        }
        public static void anlylog() {
            try
            {
                file2Handle.Add(LogFileInfo.HIT_FILE);  //hit 

//                       file2Handle.Add(LogFileInfo.UV_FILE);  //uv ok!

                file2Handle.Add(LogFileInfo.PV1_FILE);
//                file2Handle.Add(LogFileInfo.PROXY_PV_FILE);
                file2Handle.Add(LogFileInfo.PV2_FILE);   //pv
                initFolders();
                string fileStrs = "";
                foreach (string f in file2Handle) fileStrs += ":" + f;

                //log.WriteLine("处理文件{0}", fileStrs.Substring(1));

                Console.WriteLine("处理文件{0}", fileStrs.Substring(1));
                //方法1
                /**
                move2NormalParallel();
                Console.WriteLine("解压日期：{0}完成", date2Handle);
                Console.ReadKey();
                return;
                **/


                List<Thread> analyzedThreads = new List<Thread>();
                for (int i = 0; i < AnlyzeThreadCnt; i++) //启动解析线程
                {
                    Program p = new Program();
                    Thread td = new Thread(p.LogAnly);
                    analyzedThreads.Add(td);
                    ThreadInfo t = new ThreadInfo();
                    t.FileQueue = logfiles;
                    td.Start(t);

                }

                /*   
             OutObj tPvObj = new OutObj();
             tPvObj.Queue = pvDwFileQueue;
             tPvObj.OutStream = pv2f;
             tPvObj.Batch = 100000;

             Thread thPvW = new Thread(PipelineStages.Write2DwFileThread);
             thPvW.Start(tPvObj);





                OutObj tPv2Obj = new OutObj();
                tPv2Obj.Queue = pv2DwFileQueue;
                tPv2Obj.OutStream = pv22f;
                tPv2Obj.Batch = 100000;

                Thread thPv2 = new Thread(PipelineStages.Write2DwFileThread);
                thPv2.Start(tPv2Obj);
               */
                /*

                            OutObj tUvObj = new OutObj();
                            tUvObj.Queue = uvDwFileQueue;
                            tUvObj.OutStream = uv2f;
                            tUvObj.Batch = 100000;

                            Thread thUv = new Thread(PipelineStages.Write2DwFileThread);
                            thUv.Start(tUvObj);
                            */
                
                               OutObj tHitObj = new OutObj();
                               tHitObj.Queue = hitDwFileQueue;
                               tHitObj.Hbasequeue = hitDwQueue;
                               tHitObj.OutStream = hit2f;
                               tHitObj.Model = hitModel;
                               tHitObj.Batch = 100000;
                /*
                             Thread thHit = new Thread(PipelineStages.Write2DwFileThread);
                             thHit.Start(tHitObj);
                             */


                              Thread thHit = new Thread(PipelineStages.Write2Dw);
                              thHit.Start(tHitObj);
                               





                /*
                List<Task> wrtite2HbaseTasks = new List<Task>();
                for (int i = 0; i < Write2HbaseThreadCnt; i++) {
                    Task task = PipelineStages.WriteToHbase(tPvObj);
                    wrtite2HbaseTasks.Add(task);
                }
                */

                //第一种方法
                /*
                 var dirs = Directory.GetDirectories(correct_dir);
                 LoadDirParallel(dirs);
                 */

                //方法2

                move2NormalParallelAndEnqueue();

                addCompleted = true;

                foreach (Thread t in analyzedThreads)
                {
                    t.Join();
                }

                anlyzeCompleted = true;
               // thPvW.Join();
                //thPv2.Join();
                //  thUv.Join();

               thHit.Join();


                /*
                readFile(@"D:\test-data\0802\PV.txt", "PV");
                addCompleted = true;
                foreach (Task task in wrtite2HbaseTasks) {
                    task.Wait();
                }
                */

                //   WaitOutput();
                allCompleted = true;

            }
            finally
            {
                uv2f.Flush();
                uv2f.Close();
                pv2f.Flush();
                pv2f.Close();
                pv22f.Flush();
                pv22f.Close();
                hit2f.Flush();
                hit2f.Close();

            }

        }
        public static void readFile2Dw(string day_id)
        {
            loadHitFile(dataDirRoot + day_id.Substring(4) + "\\hit.txt", "HIT");
        }


        public static void testHbaseWrite(string day_id) {

            OutObj tPvObj = new OutObj();
            tPvObj.Hbasequeue = pvHbaseQueue;

            List<Task> wrtite2HbaseTasks = new List<Task>();
            for (int i = 0; i < Write2HbaseThreadCnt; i++)
            {
                Task task = PipelineStages.WriteToHbase(tPvObj);
                wrtite2HbaseTasks.Add(task);
            }

            readFile(@"e:\\test-data\\20160817\\PV.txt", "PV");

            addCompleted = true;

            foreach (Task task in wrtite2HbaseTasks)
            {
                task.Wait();
            }
            allCompleted = true;

        }

        public static void LoadDirParallel(string[] dirs)
        {

            Parallel.ForEach(dirs, (fdir) =>
            {
                var flogs = Directory.GetFiles(fdir);
                EnqueueByDirAndFiles(fdir.Substring(fdir.LastIndexOf(@"\")+1), flogs);
            });
        }
        public static void loadHitFile(string fileName, string type)
        {
            StreamReader reader = null;
            Console.WriteLine(fileName);
            string line = null;
            try
            {
                FileStream fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
                Console.WriteLine(Hit.DW_TABLE);
                reader = new StreamReader(fs);
                using (SqlConnection conn = new SqlConnection(cnstr))
                {
                    conn.Open();
                    DataTable dt = new DataTable();
                    hitModel.SetDataTableColumnsFromDB(dt, conn, Hit.DW_TABLE);
                    int count = 0;

                    while ((line = reader.ReadLine()) != null)
                    {
                        hitModel.SetDataRow(dt, line.Split('\t'));
                        count++;
                        if (count %100000 == 0) {
                            Console.WriteLine(DateTime.Now+" "+count);
                        }
                        if (count == step)
                        {//提交数据  
                            DataExtUtil.BatchCopyDataToSqlDw(step, dt, conn,Hit.DW_TABLE);
                            count = 0;
                        }

                    }
                    if (dt.Rows.Count > 0)
                    {//补充提交尾巴数据
                        DataExtUtil.BatchCopyDataToSqlDw(step, dt, conn, Hit.DW_TABLE);
                        count = 0;
                    }
                    conn.Close();
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.StackTrace);
                WriteLog(e);
                Console.ReadKey();
            }
            finally
            {
                if (reader != null)
                    reader.Close();
            }

        }


        public static void readFile(string fileName, string type) {
            StreamReader reader = null;
            string line = null;
            try
            {
                FileStream fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
                reader = new StreamReader(fs);

                while ((line = reader.ReadLine()) != null)
                {
                        Arr2HbaseQueue(line);
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.StackTrace);
                WriteLog(e);
            }
            finally
            {
                if (reader != null)
                    reader.Close();
            }

        }

   


        public static void Arr2HbaseQueue(string line) {

            string[] arr = line.Split('\t');
            if (pvHbaseQueue.Count > 3000000)
                Thread.Sleep(100);
            try
            {
              
                BsonDocument bson = new BsonDocument();
                bson.Add(PV.DMAC, arr[0]);
                bson.Add(PV.MAC, arr[1]);
                bson.Add(PV.IP, arr[2]);
                bson.Add(PV.DATETIME_POINT, arr[3]);
                bson.Add(PV.CLIENT_AGENT, arr[4]);
                bson.Add(PV.HTTP_METHOD, arr[5]);
                bson.Add(PV.HTTP_URI, arr[6]);
                bson.Add(PV.HTTP_VERSION, arr[7]);
                bson.Add(PV.REFERER, arr[8]);
                bson.Add(PV.CLIENT_OS, arr[9]);
                bson.Add(PV.MOBILE_BRAND, arr[10]);
                bson.Add(PV.CLIENT_BROWSER, arr[11]);
                bson.Add(PV.DAY_ID, arr[12]);
                bson.Add(PV.IN_DB_DATETIME,long.Parse(arr[13]));
                DateTime date = DateTime.ParseExact(arr[3], "yyyy-MM-dd HH:mm:ss", null);
                for (int i = 0; i < 50; i++)
                {
                 string rowkey = ConvertUtil.getHbaseRowKeyUnique(date, arr[0]);
                 bson.Add(PV.ROW_KEY, rowkey);
                 pvHbaseQueue.Enqueue(bson);
                 bson = bson.Clone().AsBsonDocument;
                 bson.Remove(PV.ROW_KEY);

                }

            }
            catch (Exception e)
            {
                Console.Error.WriteLine(arr[3]);
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.StackTrace);
                WriteLog(e);
            }
            
        }
        public void stataicTestRandom() {
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < 2000000; i++)
            {
                /*
                lock(syncHitF){
                    sb.Append(PipelineStages.rand.Next(100).ToString("D2")).Append("/n");
                }
                */
                sb.Append(ConvertUtil.getRealNumber(100).ToString("D2")).Append("\n");
                if (i % 500000 == 0)
                    lock (syncFileCount)
                    {
                        log.WriteLine(sb.ToString());
                        log.Flush();
                        sb = new StringBuilder();
                    }

            }
            lock (syncFileCount)
            {
                Console.WriteLine("线程{0} 完成", Thread.CurrentThread.ManagedThreadId);
            }
        }
        public void LogAnly(Object obj)
        {
           
            var input = obj as ThreadInfo;
            IHBaseModel model = null;
            while (true)
            {

                if (input.FileQueue.Count > 0)
                {
                    LogFileInfo t = null;
                    Boolean geted = false;

                    geted = input.FileQueue.TryDequeue(out t);
                    if (geted)
                    {
                        lock (syncFileCount)
                        {
                            handleFileCnt++;
                        }
                        switch (t.GetLogType())
                        {
                            case LogFileInfo.FILE_TYPE_PV:
                                model = pvModel;
                                lock (syncPvF)
                                {
                                    handlePvFileCnt++;
                                }
                                if (hit_dmac.Contains(t.Dmac))
                                    continue;
                                break;
                            case LogFileInfo.FILE_TYPE_PV2:
                                model = pv2Model;
                                lock (syncPv2F)
                                {
                                    handlePv2FileCnt++;
                                }
                                break;
                            case LogFileInfo.FILE_TYPE_UV:
                                model = uvModel;
                                lock (syncUvF)
                                {
                                    handleUvFileCnt++;
                                }
                                break;
                            case LogFileInfo.FILE_TYPE_HIT:
                                model = hitModel;
                                lock (syncHitF)
                                {
                                    handleHitFileCnt++;
                                  
                                }
                                    hit_dmac.Add(t.Dmac);
                                break;
                        }

                        if (model != null) handleLog(t, model);

                    }
                }
                else if (Program.addCompleted == true)
                {
                    break;
                }
                else
                {//如果没有文件要处理，休30秒
                    Console.WriteLine("解析线程{0}，等待新文件,休眠{1}秒钟", Thread.CurrentThread.ManagedThreadId, 10);
                    Thread.Sleep(1000 * 10);
                }
            }

        }


        private static bool IsTrain(string dmac)
        {
            return Regex.IsMatch(dmac.ToUpper(), "^(GD200|HM)");
        }
        public static Boolean IsValidTar(string tarName)
        {
                          
            if (Regex.IsMatch(tarName.Replace("-", "").Replace(":",""), validTarStr))
            {
             //   Console.WriteLine("{0} {1} {2}", true, tarName, validTarStr);
                return true;
            }
            else {
             // Console.WriteLine("{0} {1} {2}", false, tarName, validTarStr);
                //  Console.Error.WriteLine("非法的日期文件:" + tarName);
                return false;
            }


        }



        public static void handleLog(LogFileInfo f, IHBaseModel model)
        {
            StreamReader reader = null;
            try
            {
                reader = new StreamReader(f.Fullname);
                //              await  handleLog2Hbase(f.Dmac, reader,"","", model);
              //  handleLog4Dw(f.Dmac, reader, "", "", model);
              handleLog4WithTransformDw(f.Dmac, reader, "", "", model);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.StackTrace);
                WriteLog(e);
            }
            finally
            {
                if (reader != null)
                    reader.Close();
            }

        }
        public static async Task handleLog2Hbase(string dmac, StreamReader reader, string blobName, string fileName, IHBaseModel model)
        {
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
                        if (ids.Add(data.GetValue(ConvertUtil.Unique_Key).ToString()))
                        {
                            data.Remove(ConvertUtil.Unique_Key);
                            pvHbaseQueue.Enqueue(data);
                        }

                    }

                    if (model.shouldInToDB(list.Count))
                    {
                        List<BsonDocument> toHbase = list;
                        list = new List<BsonDocument>();
                        Console.WriteLine("写入hbase:" + toHbase.Count);
                        await HBaseBLL.BatchInsertJsonAsync(toHbase, model);
                    }


                }
                catch (Exception e)
                {
                    Trace.TraceError(e.Message + "\r\n" + e.StackTrace);
                    CommonUtil.LogException(dmac, e);
                    Console.ReadKey();
                }


            }
            if (pvHbaseQueue.Count > 20 * pvModel.BATHCH) //解析停止3秒钟
            {
                Console.WriteLine("解析线程{0} 休眠{1}秒钟", Thread.CurrentThread.ManagedThreadId, 3);
                Thread.Sleep(1000 * 3);
            }


            if (list.Count > 0)
            {

                try
                {
                    Console.WriteLine("写入hbase:" + list.Count);
                    await HBaseBLL.BatchInsertJsonAsync(list, model);
                }
                catch (Exception e)
                {
                    Console.ReadKey();
                    CommonUtil.LogException(dmac, e);
                }
                bu_count = ids.Count;
                ids.Clear();
                list.Clear();
            }



        }

        public static void handleLog4WithTransformDw(string dmac, StreamReader reader, string blobName, string fileName, IHBaseModel model)
        {
            List<BsonDocument> list = new List<BsonDocument>();
            StringBuilder sb = new StringBuilder();
            string line = null;
            HashSet<string> ids = new HashSet<string>();
           // ConcurrentQueue<string> queue = null;
            int rcnt = 0;

            //   queue = model.GetDwFileQueue();
          
            try
            {
                while ((line = reader.ReadLine()) != null)
                {
                    if (model.GetColumnFamily() == "PV" && !line.Contains("hitID")) { continue; }
                    BsonDocument doc = model.LineToBson(line, dmac);

                    if (doc != null)
                    {

                        string id = doc.GetValue("_id").AsString;
                        rcnt++;
                        if (ids.Add(id))
                        {

                            if (model.GetColumnFamily() == "PV")
                            {
                                string url = doc.GetValue(PV.HTTP_URI).AsString;
                                int pos = url.IndexOf("hitID");
                                if (pos > 0)
                                {
                                    //                                    Trace.TraceError("解析hitID");
                                    BsonDocument hit = extractHitFromUrl(url.Substring(pos), dmac, doc);

                                    if (hit != null)
                                    {
                                        hitDwQueue.Enqueue(hit); 
                                    }
                                }
                                continue;
                            }

                            hitDwQueue.Enqueue(doc);

                        }


                    }
                }
                model.AddCnt(ids.Count);
                model.AddRCnt(rcnt);
                ids.Clear();

                while (hitDwQueue.Count > 5 * 100000)
                {
                    Console.WriteLine("解析线程{0} 等待输出 休眠{1}秒钟", Thread.CurrentThread.ManagedThreadId, 5);
                    Thread.Sleep(1000 * 5);
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                WriteLog(e);
            }

        }

        private static BsonDocument extractHitFromUrl(string url, string dmac, BsonDocument data)
        {
            string[] str = url.Split('&');
            string json = "";
            Boolean notime = true;
            foreach (var tmp in str)
            {
                string[] item = tmp.Split('=');
                if (item[0] == "t" || item[0] == "start_time")
                {
                    json += ",\"time\":" + item[1];
                    notime = false;
                }
                else
                {
                    json += ",\"" + item[0] + "\":\"" + item[1] + "\"";
                }
            }
            if (notime) return null;

            if (!json.Contains("\"mac\":\"")) json += ",\"" + Hit.MAC + "\":\"" + data.GetValue(PV.MAC, "").AsString + "\"";
            json += ",\"" + Hit.IP + "\":\"" + data.GetValue(PV.IP, "").AsString.Trim() + "\"";
            json += ",\"" + Hit.USER_AGENT + "\":\"" + data.GetValue(PV.CLIENT_AGENT, "").AsString.Trim() + "\"";
            json = "{" + json.Substring(1) + "}";
            //            Trace.TraceError(" hit json " + json);
           // return json;
            return hitModel.LineToBson(json, dmac);
        }
        public static void handleLog4Dw(string dmac, StreamReader reader, string blobName, string fileName, IHBaseModel model)
        {
            List<BsonDocument> list = new List<BsonDocument>();
            StringBuilder sb = new StringBuilder();
            string line = null;
            HashSet<string> ids = new HashSet<string>();
            ConcurrentQueue<string> queue = null;
            int rcnt = 0;

            queue = model.GetDwFileQueue();

            try
            {
                while ((line = reader.ReadLine()) != null)
                {
                  
                    BsonDocument doc = model.LineToBson(line, dmac);
                  
                    if (doc != null)
                    {

                        string id = doc.GetValue("_id").AsString;
                        rcnt++;
                        if (ids.Add(id))
                        {
                            try
                            {
                                model.BsonToDw(doc, sb);
                                queue.Enqueue(sb.ToString());
                                sb.Clear();
                            }
                            catch (Exception e)
                            {
                                //                          Console.WriteLine(e.Message);
                                //                          Console.WriteLine(e.StackTrace);
                                //                          Console.WriteLine(line);
                                //                          Console.WriteLine(doc);
                                WriteLog(e);

                            }

                        }


                    }
                }
                model.AddCnt(ids.Count);
                model.AddRCnt(rcnt);
                ids.Clear();

                while (queue.Count > 4 * 100000)
                {
                    Console.WriteLine("解析线程{0} 休眠{1}秒钟", Thread.CurrentThread.ManagedThreadId, 5);
                    Thread.Sleep(1000 * 5);
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                WriteLog(e);
            }



        }


        public static void EnqueueByDirAndFiles(string dirName, string[] flogs)
        {
            try {
                string dmac;
                if (dirName.ToUpper().StartsWith("GD")) //获取dmac
                { //高达
                    dmac = dirName.Substring(0, dirName.IndexOf("-"));
                }
                else if (dirName.StartsWith("_"))
                { //形如 -2016-08-07-16-02-03.tar.gz的文件 经过上一步程序处理
                    dmac = dirName.Substring(1, dirName.IndexOf("-") - 1);
                }
                else
                {
                    dmac = dirName.Substring(0, 18).Replace("-", "");
                }
                 Array.Sort(flogs);
                foreach (var f in flogs)                {
                    LogFileInfo t = new LogFileInfo();
                    t.Fname = f.Substring(f.LastIndexOf(@"\") + 1);

                    if (!file2Handle.Contains(t.Fname)) continue;

                    t.Dmac = dmac;
                    t.Fullname = f;
                    //Console.WriteLine(t.Dmac);
                    if (String.IsNullOrEmpty(t.Dmac))
                    {
                        Console.Error.WriteLine("dmac异常,dmac={0}", t.Dmac);
                        Console.ReadKey();
                    }
                    //       Console.WriteLine("dmac="+t.Dmac);  
                    //      Console.WriteLine("get---->" + t.Fname);
                    logfiles.Enqueue(t);
                    //      Console.WriteLine(logfiles.Count);

                }
            }catch(Exception e){
                Console.WriteLine(e.Message + "\r\n" + e.StackTrace);
                WriteLog(e);
            } 
         

        }

        public static void mem_Enqueue()
        {

            String src_path = orgin_dir;

            //第一种方法
            var dirs = Directory.GetDirectories(src_path);
            int cnt = 0;

            foreach (var dir in dirs)
            {
                cnt++;
                Console.WriteLine("已处理{0}个目录，正在处理目录{1}", cnt, dir);
                var files = Directory.GetFiles(dir);

                Parallel.ForEach(files, (file) =>
                {
                    if (!IsValidTar(file)) return;

                    string outDir = "";
                    string fname = file.Substring(file.LastIndexOf("\\") + 1);
                    string fdir = fname.Substring(0, fname.Length - 7);

                    if (fdir.StartsWith("-")) fdir = "_" + dir.Substring(dir.LastIndexOf("\\") + 1).ToUpper() + fdir;
                    if (fdir.StartsWith("sys")) fdir = fdir.Substring(4);  //sys-58-69-6C-03-C7-D6-16-06-14-10-00-32.tar.gz

                    FileStream fs = null;
                    GZipInputStream gzStream = null;
                    TarInputStream tarStream = null;
                    
                    string outfile = null;
                    try
                    {
                        fs = new FileStream(file, FileMode.Open);
                        gzStream = new GZipInputStream(fs);
                        tarStream = new TarInputStream(gzStream);

                        List<Stream> outers = new List<Stream>();
                        TarEntry entry = tarStream.GetNextEntry();

                        while (entry != null)
                        {
                            Stream streamOut = new MemoryStream();

                            string fileName = entry.Name.Substring(entry.Name.LastIndexOf("/") + 1);
                            outfile = outDir + "\\" + fileName;
                            //  Console.WriteLine(outfile);
                            //  Console.WriteLine(fileName);

                            if (!file2Handle.Contains(fileName))
                            {
                                entry = tarStream.GetNextEntry();
                                continue;
                            }
                           
                            tarStream.CopyEntryContents(streamOut);
                            entry = tarStream.GetNextEntry();
                            memQueue.Enqueue(streamOut);
                            if (current_mem_que_size < MAX_CACHE)
                            {
                                Interlocked.Add(ref current_mem_que_size, streamOut.Length);
                            }
                            while (current_mem_que_size >= MAX_CACHE) {
                                Thread.Sleep(1000);
                            }
                              
                        }
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(outfile + "\n" + e.Message + "\n" + e.StackTrace);
                        WriteLog(e);
                    }
                    finally
                    {
                        if (fs != null) fs.Close();
                    }


                });
            }


        }


        public static void move2NormalParallelAndEnqueue()
        {

            String src_path = orgin_dir;
            String cor_path = correct_dir;

            //第一种方法
            var dirs = Directory.GetDirectories(src_path);
            int cnt = 0;

            foreach (var dir in dirs)
            {
                cnt++;
                Console.WriteLine("已处理{0}个目录，正在处理目录{1}", cnt, dir);
                var files = Directory.GetFiles(dir);

                Parallel.ForEach(files, (file) =>
                {
                    if (!IsValidTar(file)) return;

                    string outDir = "";
                    string fname = file.Substring(file.LastIndexOf("\\") + 1);
                    string fdir = fname.Substring(0, fname.Length - 7);

                    if (fdir.StartsWith("-")) fdir = "_" + dir.Substring(dir.LastIndexOf("\\") + 1).ToUpper() + fdir;
                    if (fdir.StartsWith("sys")) fdir = fdir.Substring(4);  //sys-58-69-6C-03-C7-D6-16-06-14-10-00-32.tar.gz

                    outDir = cor_path + "\\" + fdir;


                    try
                    {
                        string path = outDir;                   
                        if(Directory.Exists(path))
                        {
                            var flogs = Directory.GetFiles(path);
                            EnqueueByDirAndFiles(fdir, flogs);
                            return; //如果已经存在，则认为已解压，目前这样，每次只能处理同一种类型的文件
                        }

                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine("path=" + outDir);
                        Console.Error.WriteLine(e.Message + "\n" + e.StackTrace);
                        WriteLog(e);
                    }
                    FileStream fs = null;
                    GZipInputStream gzStream = null;
                    TarInputStream tarStream = null;
                    List<string> logs = new List<string>();
                    string outfile = null;
                    try
                    {
                        fs = new FileStream(file, FileMode.Open);
                        gzStream = new GZipInputStream(fs);
                        tarStream = new TarInputStream(gzStream);

                        List<Stream> outers = new List<Stream>();
                        TarEntry entry = tarStream.GetNextEntry();

                        while (entry != null)
                        {
                            Stream streamOut = null;
                            
                            string fileName = entry.Name.Substring(entry.Name.LastIndexOf("/") + 1);
                            outfile = outDir + "\\" + fileName;
                            //  Console.WriteLine(outfile);
                            //  Console.WriteLine(fileName);

                            if (!file2Handle.Contains(fileName))
                            {
                                entry = tarStream.GetNextEntry();
                                continue;
                            }

                            if (!Directory.Exists(outDir))
                            {//判断是否存在
                                Directory.CreateDirectory(outDir);//创建新路径
                            }

                            if (File.Exists(outfile))
                            {
                                logs.Add(outfile);
                                if (logs.Count > 10000)
                                {
                                    EnqueueByDirAndFiles(fdir, logs.ToArray());
                                    logs = new List<string>();
                                }
                                entry = tarStream.GetNextEntry();
                                continue;
                            }
                            using (streamOut = new FileStream(outfile, FileMode.CreateNew)) {
                                tarStream.CopyEntryContents(streamOut);
                                logs.Add(outfile);
                                if (logs.Count > 10000)
                                {
                                    EnqueueByDirAndFiles(fdir, logs.ToArray());
                                    logs = new List<string>();
                                }
                                entry = tarStream.GetNextEntry();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(outfile + "\n" + e.Message + "\n" + e.StackTrace);
                        WriteLog(e);
                    }
                    finally {
                        if (fs != null) fs.Close();
                    }

                  if(logs.Count>0) EnqueueByDirAndFiles(fdir, logs.ToArray());

                });
            }


        }


        public static void move2NormalParallel()
        {

            String src_path = orgin_dir;
            String cor_path = correct_dir;

            //第一种方法
            var dirs = Directory.GetDirectories(src_path);
            int cnt = 0;

            foreach (var dir in dirs)
            {
                cnt++;
                Console.WriteLine("已处理{0}个目录，正在处理目录{1}", cnt, dir);
                var files = Directory.GetFiles(dir);

                Parallel.ForEach(files, (file) =>
                {
                if (!IsValidTar(file)) return;

                string outDir = "";
                string fname = file.Substring(file.LastIndexOf("\\") + 1);
                string fdir = fname.Substring(0, fname.Length - 7);

                if (fdir.StartsWith("-")) fdir = "_" + dir.Substring(dir.LastIndexOf("\\") + 1).ToUpper() + fdir;
                if (fdir.StartsWith("sys")) fdir = fdir.Substring(4);  //sys-58-69-6C-03-C7-D6-16-06-14-10-00-32.tar.gz

                outDir = cor_path + "\\" + fdir;

                try
                {
                    string path = outDir;
                    if (!Directory.Exists(path))
                    {//判断是否存在
                        Directory.CreateDirectory(path);//创建新路径
                    }
                    else return; //如果已经存在，则认为已解压
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine("path=" + outDir);
                    Console.Error.WriteLine(e.Message + "\n" + e.StackTrace);
                    WriteLog(e);
                }
                FileStream fs = null;
                GZipInputStream gzStream = null;
                TarInputStream tarStream = null;
                List<string> logs = new List<string>();
                string outfile = null;
                try
                {
                    fs = new FileStream(file, FileMode.Open);
                    gzStream = new GZipInputStream(fs);
                    tarStream = new TarInputStream(gzStream);

                    List<Stream> outers = new List<Stream>();
                    TarEntry entry = tarStream.GetNextEntry();
                    while (entry != null)
                    {
                        Stream streamOut = null;

                        string fileName = entry.Name.Substring(entry.Name.LastIndexOf("/") + 1);
                        outfile = outDir + "\\" + fileName;
                        if (!file2Handle.Contains(fileName)) {
                            entry = tarStream.GetNextEntry();
                            continue;
                        }
                        if (File.Exists(outfile))
                        {
                            entry = tarStream.GetNextEntry();
                            continue;
                        }
                        using (streamOut = new FileStream(outfile, FileMode.CreateNew)) {

                            tarStream.CopyEntryContents(streamOut);
                            entry = tarStream.GetNextEntry();

                        }
                               
                      }
        

                 } catch (Exception e){
                    Console.Error.WriteLine(outfile + "\n" + e.Message + "\n" + e.StackTrace);
                    WriteLog(e);
                }
                finally
                {
                    if (fs != null) fs.Close();
                       
                }
                 
                });
            }


        }



        public static void WriteLog(Exception e)
        {
            lock (log_err)
            {
                //获得字节数组
                byte[] data = System.Text.Encoding.Default.GetBytes(e.Message + "\n" + e.StackTrace);
                //开始写入
                log_err.Write(data, 0, data.Length);
                //清空缓冲区、关闭流
                log_err.Flush();
            }
        }



        public void listFile()
        {

            String path = @"G:\temp";

            //第一种方法
            var files = Directory.GetDirectories(path);

            foreach (var file in files)
            {
                var fs = Directory.GetFiles(file);
                foreach (var f in fs)
                {
                    Console.WriteLine(f.Substring(8));

                }

            }

            Console.ReadKey();
            //第二种方法
            DirectoryInfo folder = new DirectoryInfo(path);

            foreach (FileInfo file in folder.GetFiles("*.tar.gz"))
            {
                Console.WriteLine(file.FullName);
            }

        }

        public static void DownloadBlob(string day_id)
        {

            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference(day_id);

            // Loop over items within the container and output the length and URI.
            List<Task<bool>> tasks = new List<Task<bool>>();
            foreach (IListBlobItem item in container.ListBlobs(null, true))
            {
                if (item.GetType() == typeof(CloudBlockBlob))
                {
                    CloudBlockBlob blob = (CloudBlockBlob)item;
                    string tarName = blob.Name.Substring(blob.Name.IndexOf("/") + 1);
       //             Console.WriteLine(blob.Properties.LastModified.Value.LocalDateTime.ToString("yyyyMMddHH"));
      //              if (blob.Properties.LastModified.Value.LocalDateTime.ToString("yyyyMMddHH").Substring(0,date2Handle.Length).Equals(date2Handle))     
                    if (IsValidTar(tarName))
                    {
                        if (tasks.Count < 1000)
                        {
                            // Task<bool> t = downloadSpecifyBlob(blob, @"e:\blob\20160826\" + blob.Name.Substring(blob.Name.IndexOf("/") + 1));
                            string path = driver +"/"+date2Handle+blob.Uri.ToString().Substring(blob.Uri.ToString().IndexOf("/"+day_id) +9);
                            if (!Directory.Exists(path.Substring(0, path.LastIndexOf("/"))))//如果目录不存在，则创建
                            {
                                Console.WriteLine("path=" + path);
                                Directory.CreateDirectory(path.Substring(0, path.LastIndexOf("/")));
                            }
                            if (downloadDir.Add(path.Substring(0, path.LastIndexOf("/"))))
                                    Console.WriteLine("已下载{0}个目录", downloadDir.Count);
                            Task<bool> t = downloadSpecifyBlob(blob,path);
                            tasks.Add(t);                          
                            continue;
                        }
                        waitNext(tasks, 1000);

                        //      byte[] bytes = new byte[blob.Properties.Length];
                        //      blob.DownloadToByteArrayAsync(bytes, 0).Wait();
                        //      GZipHelper.GZipFile("123", bytes, blob.Name); 

                        //     Console.WriteLine("Block blob of length {0}: {1} name={2} valide={3}", blob.Properties.Length, blob.Uri, tarName, IsValidTar(tarName));
                        // Console.ReadKey();


                    }
                    else if (item.GetType() == typeof(CloudPageBlob))
                    {
                        CloudPageBlob pageBlob = (CloudPageBlob)item;

                        Console.WriteLine("Page blob of length {0}: {1}", pageBlob.Properties.Length, pageBlob.Uri);


                    }
                    else if (item.GetType() == typeof(CloudBlobDirectory))
                    {
                        CloudBlobDirectory directory = (CloudBlobDirectory)item;

                        Console.WriteLine("Directory: {0}", directory.Uri);
                    }
                }
            }
        }
        private static void waitNext(List<Task<bool>> tasks,int max_task_num) {
            while (tasks.Count >= max_task_num) {
                int i = tasks.Count - 1;
                do
                {
                  //  Console.WriteLine(tasks[i].Status + " c：" + tasks[i].IsCompleted + " f:" + tasks[i].IsFaulted + " r:" + tasks[i].Result);
                    if (tasks[i].IsCompleted)
                    {
                        tasks.RemoveAt(i);
                    }
                    i--;
                } while (i >= 0);
                if (tasks.Count >= max_task_num) {
                    Console.WriteLine("主线程休眠1秒");
                    Thread.Sleep(1000);
                    //  Task.Delay(1000*60); 
                }
            }

        }

        public static void listBlob()
        {
            
            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference("20160825");

            // Loop over items within the container and output the length and URI.
            foreach (IListBlobItem item in container.ListBlobs(null, true))
            {
                if (item.GetType() == typeof(CloudBlockBlob))
                {
                    CloudBlockBlob blob = (CloudBlockBlob)item;
                    //       downloadSpecifyBlob(blob, @"d:\blob\"+blob.Name.Substring(blob.Name.IndexOf("/")+1));

                    //      byte[] bytes = new byte[blob.Properties.Length];
                    //      blob.DownloadToByteArrayAsync(bytes, 0).Wait();
                    //      GZipHelper.GZipFile("123", bytes, blob.Name); 

                    Console.WriteLine("Block blob of length {0}: {1} ", blob.Properties.Length, blob.Uri);
                    Console.WriteLine(blob.Uri.ToString().Substring(blob.Uri.ToString().IndexOf("/20160825") + 10));
                    Console.ReadKey();

                }
                else if (item.GetType() == typeof(CloudPageBlob))
                {
                    CloudPageBlob pageBlob = (CloudPageBlob)item;

                    Console.WriteLine("Page blob of length {0}: {1}", pageBlob.Properties.Length, pageBlob.Uri);


                }
                else if (item.GetType() == typeof(CloudBlobDirectory))
                {
                    CloudBlobDirectory directory = (CloudBlobDirectory)item;

                    Console.WriteLine("Directory: {0}", directory.Uri);
                }
            }
        }


        public static Task<bool> downloadSpecifyBlob(CloudBlockBlob blockBlob, string path)
        {
            if (File.Exists(path)) { return Task.Run(() => { return true; }); };
                
            return Task.Run(async () =>
            {
                using (var fileStream = System.IO.File.Open(path, FileMode.OpenOrCreate))
                {
                    await blockBlob.DownloadToStreamAsync(fileStream);
                }
                return true;
            });
           

        }

        public static void downloadBlob()
        {

            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference("20160602");

            // Retrieve reference to a blob named "photo1.jpg".
            CloudBlockBlob blockBlob = container.GetBlockBlobReference("00-08-D2-EB-B2-52-16-06-02-07-23-30.tar.gz");

            // Save blob contents to a file.
            using (var fileStream = System.IO.File.OpenWrite(@"G:\temp\myfile"))
            {
                blockBlob.DownloadToStream(fileStream);
            }
        }

    }
}
