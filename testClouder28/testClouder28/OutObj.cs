using MongoDB.Bson;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace testClouder28
{
    class OutObj
    {
        private ConcurrentQueue<string> queue;
        private ConcurrentQueue<BsonDocument> hbasequeue;
        private StreamWriter outStream;
        private int batch;
        private string logType;



        public ConcurrentQueue<string> Queue
        {
            get
            {
                return queue;
            }

            set
            {
                queue = value;
            }
        }

      

        public int Batch
        {
            get
            {
                return batch;
            }

            set
            {
                batch = value;
            }
        }

        public StreamWriter OutStream
        {
            get
            {
                return outStream;
            }

            set
            {
                outStream = value;
            }
        }

        public ConcurrentQueue<BsonDocument> Hbasequeue
        {
            get
            {
                return hbasequeue;
            }

            set
            {
                hbasequeue = value;
            }
        }

        public string LogType
        {
            get
            {
                return logType;
            }

            set
            {
                logType = value;
            }
        }
    }
}
