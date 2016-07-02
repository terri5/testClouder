
using AlalyzeLog.DBTools;
using AlalyzeLog.Worker.Model;
using analyzeLogWorkRole.Model;
using AnlalyzeLog.DBTools;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace AlayzeLog.DBTools
{
    public class HBaseBLL
    {


        /// <summary>
        /// create table 
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="columnFamilysKeys"></param>
        /// <returns></returns>
        public static async Task<bool> CreateTableAsync(string tableName, string[] columnFamilysKeys)
        {
            return await HBaseHelper.CreateTableAsync(tableName, columnFamilysKeys);
        }


        public static async Task BatchInsertJsonAsync(List<BsonDocument> josnList, IHBaseModel model)
        {
            try
            {
                if (model.GetColumnFamily() == null)
                    return;

                CellSet cellSet = ToCellSet(josnList, model);

                await HBaseHelper.HbaseBatchInsertAsync(model.GetTableName(), cellSet);

            }
            catch (Exception e)
            {
                Trace.TraceError(e.Message + "\n" + e.StackTrace);
                CommonUtil.LogErrorMessage("", e);
            }
        }


 
        private static CellSet ToCellSet(List<BsonDocument> jsons, IHBaseModel model)
        {
            CellSet cellSet = new CellSet();
            foreach (var json in jsons)
            {
                CellSet.Row row = model.ToRowOfCellSet(json);
                if(row != null) cellSet.rows.Add(row);
            }
            return cellSet;
        }

    }

}