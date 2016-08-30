using AlalyzeLog.DBTools;
using AlayzeLog.DBTools;
using MongoDB.Bson;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace testClouder28
{
    internal class PipelineStages
    {
        private static int m_cnt = 0;
        private static int w_cnt = 0;
        public static int r_cnt = 0;
        private static object syncRoot = new object();
        private static object syncRootLog = new object();
        

        public static  Task WriteToTask(OutObj obj)
        {
            int i = 0;
            return Task.Run(async () =>
            {
                if (obj == null) return;
                OutObj outer = obj as OutObj;
                StringBuilder sb = new StringBuilder();
                while (!Program.allCompleted || outer.Queue.Count>0)
                {
                    string tmp = null;
                    Boolean geted = outer.Queue.TryDequeue(out tmp);
                    if (geted)
                    {
                        sb.Append(tmp);
                        if (++i % outer.Batch == 0)
                        {
                          await outer.OutStream.WriteAsync(sb.ToString());
                            sb.Clear();
                        }

                    }          

                }
            });
        }

        public static  void Write2DwFileThread(Object obj)
        {
            int i = 0;
           
            if (obj == null) return;
            OutObj outer = obj as OutObj;
            StringBuilder sb = new StringBuilder();
            while (!Program.anlyzeCompleted || outer.Queue.Count > 0)
            {
                string tmp = null;
                Boolean geted = outer.Queue.TryDequeue(out tmp);
                if (geted)
                {
                    sb.Append(tmp);
                    if (++i % outer.Batch == 0)
                    {
                        var watch = new Stopwatch();
                        watch.Start();
                        outer.OutStream.Write(sb.ToString());
                        watch.Stop();
                        Console.WriteLine("当前时间：{0} 写入文件耗时{1}s",DateTime.Now,watch.ElapsedMilliseconds / 1000);
                        sb=new StringBuilder();
                    }

                } else if (outer.Queue.Count == 0)
                {
                    Console.WriteLine("写入DwFile线程{0}睡眠{1}秒", Thread.CurrentThread.ManagedThreadId,5);
                    Thread.Sleep(1000*5);
                }

            }

            if (sb.Length > 0)
            {//处理尾巴
                var watch = new Stopwatch();
                watch.Start();
                outer.OutStream.Write(sb.ToString());
                outer.OutStream.Flush();
                watch.Stop();
                Console.WriteLine("当前时间：{0} 写入文件耗时{1}s 总写入记录数{2},写入日志类型{3}",
                    DateTime.Now, watch.ElapsedMilliseconds / 1000,i, outer.LogType);
                sb.Clear();

            }

        }

        public static void Write2Dw(Object obj)
        {
           
            if (obj == null) return;
            OutObj outer = obj as OutObj;
            StringBuilder sb = new StringBuilder();
            using (SqlConnection conn = new SqlConnection(Program.cnstr)) {
                conn.Open();
                DataTable dt = new DataTable();
                outer.Model.SetDataTableColumnsFromDB(dt, conn,outer.Model.getDwTable());
                long count = 0;

                while (!Program.anlyzeCompleted || outer.Hbasequeue.Count > 0)
                {
                    BsonDocument tmp = null;
                    Boolean geted = outer.Hbasequeue.TryDequeue(out tmp);
                    if (geted) {
                        outer.Model.SetDataRow(dt,tmp);
                        count++;
                        /**
                        if (count % 100000 == 0)
                        {
                            Console.WriteLine(DateTime.Now + " " + count);
                        }
                       **/
                        if (count % Program.step==0)
                        {//提交数据  
                            DataExtUtil.BatchCopyDataToSqlDw(Program.step, dt, conn, outer.Model.getDwTable());
                            Console.WriteLine("{0} {1} 线程{2}写入Dw总数 {3}",DateTime.Now,outer.Model.GetColumnFamily(),Thread.CurrentThread.ManagedThreadId,count);
                        }
                    } else if (outer.Hbasequeue.Count == 0)
                    {
                        Console.WriteLine("写入Dw线程{0}睡眠{1}秒", Thread.CurrentThread.ManagedThreadId, 3);
                        Thread.Sleep(1000 * 3);
                    }
                }
                if (dt.Rows.Count > 0)
                {//补充提交尾巴数据
                    DataExtUtil.BatchCopyDataToSqlDw(Program.step, dt, conn, outer.Model.getDwTable());
                }
                conn.Close();
         }


      }
        

        public static  Task Write2DwFileTask(Object obj)
        {
            return Task.Run(async () =>
            {
                int i = 0;
                if (obj == null) return;
                OutObj outer = obj as OutObj;
                StringBuilder sb = new StringBuilder();
                while (!Program.anlyzeCompleted || outer.Queue.Count > 0)
                {
                    string tmp = null;
                    Boolean geted = outer.Queue.TryDequeue(out tmp);
                    if (geted)
                    {
                        sb.Append(tmp);
                        if (++i % outer.Batch == 0)
                        {
                            var watch = new Stopwatch();
                            watch.Start();
                            await outer.OutStream.WriteAsync(sb.ToString());
                            watch.Stop();
                     //       Console.WriteLine("写入文件耗时{0}s", watch.ElapsedMilliseconds / 1000);
                            Program.log.WriteLine("写入DwFile线程{0}睡眠{1}秒", "写入文件耗时{0}s", watch.ElapsedMilliseconds / 1000);
                            sb.Clear();
                        }

                    }
                    else if (outer.Queue.Count == 0)
                    {
                       Console.WriteLine("写入DwFile线程{0}睡眠{1}秒", Thread.CurrentThread.ManagedThreadId, 5);
                       Thread.Sleep(1000 * 5);
                    }

                }
            });
        }
   

        public static void MonitorThread(Object obj)
        {
            if (obj == null) return;

            ConcurrentQueue<LogFileInfo> queue = obj as ConcurrentQueue<LogFileInfo>;

            while (!Program.allCompleted)
            {
                Console.WriteLine("当前待处理日志文件数量:{0} 当前时间：{1},已用时{2}小时{3}分钟" , queue.Count,DateTime.Now.ToString(),(m_cnt)/60,(m_cnt)%60);
                Console.WriteLine("当前待写入记录数{0},pv:{1},pv2:{2},uv:{3} hit:{4}", (Program.pvDwFileQueue.Count + Program.pv2DwFileQueue.Count + Program.uvDwFileQueue.Count + Program.hitDwFileQueue.Count), Program.pvDwFileQueue.Count, Program.pv2DwFileQueue.Count, Program.uvDwFileQueue.Count, Program.hitDwFileQueue.Count);
                lock (syncRootLog) {
                    Program.log.WriteLine("hbase队列数据量:{0} 已写入 hbase数量{1} 当前时间 {2},已用时{3}小时{4}分钟", Program.pvHbaseQueue.Count, r_cnt, DateTime.Now.ToString(), (m_cnt) / 60, (m_cnt) % 60);
                    Program.log.WriteLine("当前待处理日志文件数量:{0} 当前时间：{1},已用时{2}小时{3}分钟", queue.Count, DateTime.Now.ToString(), (m_cnt) / 60, (m_cnt) % 60);
                    Program.log.WriteLine("当前已处理日志文件数量{0},pv:{1},pv2:{2}，uv:{3} hit:{4}", (Program.handlePvFileCnt + Program.handlePv2FileCnt + Program.handleUvFileCnt + Program.handleHitFileCnt), Program.handlePvFileCnt, Program.handlePv2FileCnt, Program.handleUvFileCnt, Program.handleHitFileCnt);
                    Program.log.WriteLine("当前待写入记录数{0},pv:{1},pv2:{2},uv:{3} hit:{4}", (Program.pvDwFileQueue.Count + Program.pv2DwFileQueue.Count + Program.uvDwFileQueue.Count + Program.hitDwFileQueue.Count), Program.pvDwFileQueue.Count, Program.pv2DwFileQueue.Count,Program.uvDwFileQueue.Count, Program.hitDwFileQueue.Count);
                    Program.log.WriteLine("去重之前的记录数：pv:{0},pv2:{1},uv:{2},hit:{3}", Program.pvModel.GetRCnt(), Program.pv2Model.GetRCnt(), Program.uvModel.GetRCnt(), Program.hitModel.GetRCnt());
                    Program.log.WriteLine("有效记录数：pv:{0},pv2:{1},uv:{2},hit:{3}", Program.pvModel.GetCnt(),Program.pv2Model.GetCnt(),Program.uvModel.GetCnt(), Program.hitModel.GetCnt());
                    Program.log.Flush();
                }
              
                m_cnt++;
                Thread.Sleep(1000*60);

                //Task.Delay(1000 * 60);
            }
        }

        internal static Task WriteToHbase(object obj)
        {
            return Task.Run(async () =>
            {
                int i = 0;
                if (obj == null) return;
                OutObj outer = obj as OutObj;
                List<BsonDocument> list = new List<BsonDocument>();
                while (!Program.allCompleted || outer.Hbasequeue.Count > 0)
                {
                    BsonDocument tmp = null;
                    Boolean geted = outer.Hbasequeue.TryDequeue(out tmp);
                    if (geted)
                    {
                        list.Add(tmp);
                        i++;
                        if (Program.pvModel.shouldInToDB(list.Count))
                        {
                            List<BsonDocument> tmplist = list;
                            list = new List<BsonDocument>();
                            lock (syncRoot) {
                                r_cnt += tmplist.Count;
                                w_cnt++;
                            }
                            Console.WriteLine("写入hbase:{0},{1}",r_cnt,tmplist.Count);
                            var watch = new Stopwatch();
                            watch.Start();
                            await HBaseBLL.BatchInsertJsonAsync(tmplist, Program.pvModel);
                            watch.Stop();
                            lock(syncRootLog) {
                                Console.WriteLine("写入hbase线程{0},写入hbase耗时{1}s", Thread.CurrentThread.ManagedThreadId, watch.ElapsedMilliseconds / 1000);
                            }
                        }
                    }
                    
                    else if (outer.Hbasequeue.Count == 0)
                    {
                       // Console.WriteLine("写入hbase线程{0}睡眠{1}秒", Thread.CurrentThread.ManagedThreadId,0.1);
                        Thread.Sleep(100);
                    }
                }
                lock (syncRoot)
                {
                    r_cnt += list.Count;
                    w_cnt++;
                }
                Console.WriteLine("写入hbase:{0}", i);
                await Task.Delay(1000);
              //  if(list.Count>0)  await HBaseBLL.BatchInsertJsonAsync(list, Program.pvModel);
                    
            });
        }
    }
}