using System.Collections.Concurrent;
using System.IO;

namespace testClouder28
{
    public class ThreadInfo
    {
        ConcurrentQueue<LogFileInfo> fileQueue;
        private int threadNum;
        StreamWriter output;


        public ConcurrentQueue<LogFileInfo> FileQueue
        {
            get
            {
                return fileQueue;
            }

            set
            {
                fileQueue = value;
            }
        }

        public StreamWriter Output
        {
            get
            {
                return output;
            }

            set
            {
                output = value;
            }
        }

        public int ThreadNum
        {
            get
            {
                return threadNum;
            }

            set
            {
                threadNum = value;
            }
        }
    }
}