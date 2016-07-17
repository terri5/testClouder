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
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        private static Object syncFileCount = new object();




        public static System.Collections.Generic.HashSet<string> file2Handle = new System.Collections.Generic.HashSet<string>();

        public static string date2Handle = "20160716";

        public const int AnlyzeThreadCnt = 48;//总解析线程数
        public const int Write2HbaseThreadCnt = 8;//总写hbase线程数

        private static StreamWriter uv2f = null;
        private static StreamWriter pv2f = null;
        private static StreamWriter hit2f = null;
        public static StreamWriter log = null;
        public static FileStream log_err = null;


        public static string driver = "D:";
        /**
        * 整理后的文件目录
        */
        public static string correct_dir = driver + @"\cor" + date2Handle.Substring(4);
        /**
         * blob文件目录
         */
        public static string orgin_dir = driver + "\\" + date2Handle.Substring(4);


        private static string dataDirRoot = @"\test-data\";

        private static void initFolders()
        {
            uv2f = new StreamWriter(new FileStream(driver + dataDirRoot + date2Handle.Substring(4) + "\\uv.log", FileMode.Append), Encoding.GetEncoding("gb2312"));
            pv2f = new StreamWriter(new FileStream(driver + dataDirRoot + date2Handle.Substring(4) + "\\pv.log", FileMode.Append), Encoding.GetEncoding("gb2312"));
            hit2f = new StreamWriter(new FileStream(driver + dataDirRoot + date2Handle.Substring(4) + "\\hit.log", FileMode.Append), Encoding.GetEncoding("gb2312"));
            log = new StreamWriter(new FileStream(driver + dataDirRoot + "exe-" + DateTime.Now.ToString("yyyy-MM-dd-HH") + ".txt", FileMode.Append), Encoding.GetEncoding("UTF-8"));
            log_err = new FileStream(driver + dataDirRoot + "err-" + DateTime.Now.ToString("yyyy-MM-dd-HH") + ".txt", FileMode.Append);
        }


        public static System.Collections.Concurrent.ConcurrentQueue<LogFileInfo> logfiles = new ConcurrentQueue<LogFileInfo>();



        public static Hit hitModel = new Hit();
        public static UV uvModel = new UV();
        public static PV pvModel = new PV();

        public static ConcurrentQueue<string> pvDwFileQueue = pvModel.GetDwFileQueue();
        public static ConcurrentQueue<string> uvDwFileQueue = uvModel.GetDwFileQueue();
        public static ConcurrentQueue<string> hitDwFileQueue = hitModel.GetDwFileQueue();

        public static ConcurrentQueue<BsonDocument> pvHbaseQueue = new ConcurrentQueue<BsonDocument>();


        public static string validTarStr = date2Handle.Substring(2, 2) + "-" + date2Handle.Substring(4, 2) + "-" + date2Handle.Substring(6, 2);
        public static int handleHitFileCnt = 0;
        public static int handlePvFileCnt = 0;
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
                initFolders();//初始化相关目录
                Stopwatch watch = new Stopwatch();
                watch.Start();//首先启动监控线程

                log.WriteLine(date2Handle + "开始时间{0},处理日期{1}", DateTime.Now, date2Handle);

                Console.WriteLine(date2Handle + "开始时间{0},处理日期{1}", DateTime.Now, date2Handle);

                Thread monitor = new Thread(PipelineStages.MonitorThread);//首先启动监控线程
                monitor.Start(logfiles);

                file2Handle.Add(LogFileInfo.HIT_FILE);  //hit 
                file2Handle.Add(LogFileInfo.UV_FILE);  //uv ok!
                file2Handle.Add(LogFileInfo.PV1_FILE);
                file2Handle.Add(LogFileInfo.PV2_FILE);   //pv

                string fileStrs = "";
                foreach (string f in file2Handle) fileStrs += ":" + f;

                log.WriteLine("处理文件{0}", fileStrs.Substring(1));

                Console.WriteLine("处理文件{0}", fileStrs.Substring(1));

                //       move2NormalParallel();
                //          Console.WriteLine("解压日期：{0}完成", date2Handle);
                //         Console.ReadKey();
                //       return;


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

                //  StartOutPutTask();


                OutObj tPvObj = new OutObj();
                tPvObj.Queue = pvDwFileQueue;
                tPvObj.OutStream = pv2f;
                tPvObj.Batch = 200000;

                Thread thPv = new Thread(PipelineStages.Write2DwFileThread);
                thPv.Start(tPvObj);

                OutObj tUvObj = new OutObj();
                tUvObj.Queue = uvDwFileQueue;
                tUvObj.OutStream = uv2f;
                tUvObj.Batch = 300000;

                Thread thUv = new Thread(PipelineStages.Write2DwFileThread);
                thUv.Start(tUvObj);


                OutObj tHitObj = new OutObj();
                tHitObj.Queue = hitDwFileQueue;
                tHitObj.OutStream = hit2f;
                tHitObj.Batch = 300000;

                Thread thHit = new Thread(PipelineStages.Write2DwFileThread);
                thHit.Start(tHitObj);


                /*
                List<Task> wrtite2HbaseTasks = new List<Task>();
                for (int i = 0; i < Write2HbaseThreadCnt; i++) {
                    Task task = PipelineStages.WriteToHbase(tPvObj);
                    wrtite2HbaseTasks.Add(task);
                }
                **/

                //第一种方法

                // var dirs = Directory.GetDirectories(correct_dir);
                // LoadDirParallel(dirs);

                move2NormalParallelAndEnqueue();
                addCompleted = true;

                foreach (Thread t in analyzedThreads)
                {
                    t.Join();
                }

                anlyzeCompleted = true;

                thPv.Join();
                thUv.Join();
                thHit.Join();



                /*
                foreach (Task task in wrtite2HbaseTasks) {
                    task.Wait();
                }
                */


                //   WaitOutput();
                allCompleted = true;

                Console.WriteLine("处理文档数量{0},pv:{1},uv:{2},hit:{3}", handleFileCnt, handlePvFileCnt, handleUvFileCnt, handleHitFileCnt);
                Console.WriteLine("处理有效记录数量 pv:{0},uv:{1},hit:{2}", pvModel.GetCnt(), uvModel.GetCnt(), hitModel.GetCnt());

                watch.Stop();
                TimeSpan timeSpan = watch.Elapsed;
                Console.WriteLine("Time expend {0} seconds", watch.ElapsedMilliseconds / 1000);
                Console.WriteLine("耗费{0}天{1}小时{2}分钟{3}秒", timeSpan.Days, timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);
                log.WriteLine("处理文档数量{0},pv:{1},uv:{2},hit:{3}", handleFileCnt, handlePvFileCnt, handleUvFileCnt, handleHitFileCnt);
                log.WriteLine("处理有效记录数量 pv:{0},uv:{1},hit:{2}", pvModel.GetCnt(), uvModel.GetCnt(), hitModel.GetCnt());
                log.WriteLine("耗费{0}天{1}小时{2}分钟{3}秒", timeSpan.Days, timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);
                Console.ReadKey();


            }
            finally
            {
                if (log_err != null)
                {
                    log_err.Flush();
                    log_err.Close();
                }
                log.Flush();
                log.Close();
                uv2f.Flush();
                uv2f.Close();
                pv2f.Flush();
                pv2f.Close();

                hit2f.Flush();
                hit2f.Close();

            }


        }
        public static void StartOutPutTask()
        {
            OutObj tPvObj = new OutObj();
            tPvObj.Queue = pvDwFileQueue;
            tPvObj.OutStream = pv2f;
            tPvObj.Batch = 500000;
            taskPv = PipelineStages.Write2DwFileTask(tPvObj);
            /**
                        OutObj tUvObj = new OutObj();
                        tUvObj.Queue = uvDwFileQueue;
                        tUvObj.OutStream = uv2f;
                        tUvObj.Batch = 10000;
                        taskUv = PipelineStages.Write2DwFileTask(tUvObj);

                        OutObj tHitObj = new OutObj();
                        tHitObj.Queue = hitDwFileQueue;
                        tHitObj.OutStream = hit2f;
                        tHitObj.Batch = 10000;
                        taskHit = PipelineStages.Write2DwFileTask(tHitObj);
            **/







        }
        public static void WaitOutput()
        {
            taskPv.Wait();
            //taskUv.Wait(); taskHit.Wait();
        }

        public static void LoadDirParallel(string[] dirs)
        {

            Parallel.ForEach(dirs, (fdir) =>
            {
                var flogs = Directory.GetFiles(fdir);
                EnqueueByDirAndFiles(fdir, flogs);
            });
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
                    Console.WriteLine("解析线程{0}，等待新文件,休眠{1}秒钟", Thread.CurrentThread.ManagedThreadId, 20);
                    Thread.Sleep(1000 * 20);
                }
            }

        }
        public static Boolean IsValidTar(string tarName)
        {
            if (tarName.Contains(validTarStr) || tarName.Contains(date2Handle))
            {
                return true;
            }
            return false;


        }



        public static void handleLog(LogFileInfo f, IHBaseModel model)
        {
            StreamReader reader = null;
            try
            {
                reader = new StreamReader(f.Fullname);
                //     await  handleLog2Hbase(f.Dmac, reader,"","", model);
                handleLog2DwFile(f.Dmac, reader, "", "", model);
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

        public static void handleLog2DwFile(string dmac, StreamReader reader, string blobName, string fileName, IHBaseModel model)
        {
            List<BsonDocument> list = new List<BsonDocument>();
            StringBuilder sb = new StringBuilder();
            string line = null;
            HashSet<string> ids = new HashSet<string>();
            ConcurrentQueue<string> queue = null;

            queue = model.GetDwFileQueue();

            try
            {
                while ((line = reader.ReadLine()) != null)
                {
                    BsonDocument doc = model.LineToBson(line, dmac);
                    if (doc != null)
                    {

                        string id = doc.GetValue("_id").AsString;
                        model.AddRCnt1();
                        if (ids.Add(id))
                        {
                            try
                            {
                                model.BsonToDw(doc, sb);
                                queue.Enqueue(sb.ToString());
                                model.AddCnt1();
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
                ids.Clear();

                while (queue.Count > 4 * 100000)
                {
                    Console.WriteLine("解析线程{0} 休眠{1}秒钟", Thread.CurrentThread.ManagedThreadId, 2);
                    Thread.Sleep(1000 * 2);
                }

            }
            catch (Exception e)
            {
                WriteLog(e);
            }



        }


        public static void EnqueueByDirAndFiles(string dirName, string[] flogs)
        {
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

            foreach (var f in flogs)
            {
                LogFileInfo t = new LogFileInfo();
                t.Fname = f.Substring(f.LastIndexOf(@"\") + 1);

                if (!file2Handle.Contains(t.Fname)) continue;

                t.Dmac = dmac;
                t.Fullname = f;
                // Console.WriteLine(t.Fname);
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
                        if (!Directory.Exists(path))
                        {//判断是否存在
                            Directory.CreateDirectory(path);//创建新路径
                        }
                        else
                        {
                            var flogs = Directory.GetFiles(path);
                            EnqueueByDirAndFiles(fdir, flogs);
                            return; //如果已经存在，则认为已解压
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
                            string outfile = null;
                            try
                            {
                                outfile = outDir + "\\" + entry.Name.Substring(entry.Name.LastIndexOf("/") + 1);

                                if (File.Exists(outfile))
                                {
                                    logs.Add(outfile);
                                    continue;
                                }
                                streamOut = new FileStream(outfile, FileMode.CreateNew);
                                tarStream.CopyEntryContents(streamOut);

                                logs.Add(outfile);

                                entry = tarStream.GetNextEntry();
                            }
                            catch (Exception e)
                            {
                                Console.Error.WriteLine(outfile + "\n" + e.Message + "\n" + e.StackTrace);

                            }
                            finally
                            {
                                if (streamOut != null) streamOut.Close();
                            }
                        }


                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.Message + "\n" + e.StackTrace);
                    }
                    finally
                    {
                        if (fs != null) fs.Close();
                        if (gzStream != null) gzStream.Close();
                    }
                    EnqueueByDirAndFiles(fdir, logs.ToArray());
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
                            string outfile = null;
                            try
                            {
                                outfile = outDir + "\\" + entry.Name.Substring(entry.Name.LastIndexOf("/") + 1);
                                if (File.Exists(outfile))
                                {
                                    continue;
                                }
                                streamOut = new FileStream(outfile, FileMode.CreateNew);
                                tarStream.CopyEntryContents(streamOut);
                                entry = tarStream.GetNextEntry();
                            }
                            catch (Exception e)
                            {
                                Console.Error.WriteLine(outfile + "\n" + e.Message + "\n" + e.StackTrace);

                            }
                            finally
                            {
                                if (streamOut != null) streamOut.Close();
                            }
                        }

                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e.Message + "\n" + e.StackTrace);
                    }
                    finally
                    {
                        if (fs != null) fs.Close();
                        if (gzStream != null) gzStream.Close();
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
        public static void listBlob()
        {

            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference("20160608");

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

                    Console.WriteLine("Block blob of length {0}: {1}", blob.Properties.Length, blob.Uri);


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


        public static void downloadSpecifyBlob(CloudBlockBlob blockBlob, string path)
        {
            using (var fileStream = System.IO.File.Open(path, FileMode.OpenOrCreate))
            {
                blockBlob.DownloadToStream(fileStream);
            }

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
