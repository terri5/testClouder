using MongoDB.Bson;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace analyzeLogWorkRole.Model
{
   public  interface IHBaseModel
    {
        CellSet.Row ToRowOfCellSet(BsonDocument model);
        string GetTableName();
        string GetColumnFamily();
        BsonDocument LineToBson(string line, string dmac);
        Boolean shouldInToDB(int count);
        void BsonToDw(BsonDocument model,StringBuilder sb);
        ConcurrentQueue<string> GetDwFileQueue();
        void AddCnt1();
        void AddRCnt1();
    }


}
