using System;
using System.IO;
using System.IO.Compression;
using System.Runtime.Serialization.Formatters.Binary;

public class GzipHelper
{
	public GzipHelper()
	{
	}

    #region 压缩|解压

    /// <summary>
    /// 压缩
    /// </summary>
    /// <typeparam name="T">压缩类型</typeparam>
    /// <param name="obj">压缩对象</param>
    /// <returns>字节</returns>
    public static byte[] Compress<T>(T obj)
    {
        byte[] bs = null;
        try
        {
            MemoryStream ms = new MemoryStream();
            BinaryFormatter bf = new BinaryFormatter();
            bf.Serialize(ms, obj);
            MemoryStream ms2 = new MemoryStream();
            Compress(ms, ms2);
            bs = ms2.ToArray();
        }
        catch (Exception ex)
        {
  //          BugReport.ReportLocalError("Compress", typeof(T).ToString(), ex);
        }
        return bs;
    }

    /// <summary>
    /// 压缩
    /// </summary>
    /// <typeparam name="T">压缩类型</typeparam>
    /// <param name="obj">压缩对象</param>
    /// <returns>流</returns>
    public static Stream StreamCompress<T>(T obj)
    {
        MemoryStream ms = null;
        try
        {
            ms = new MemoryStream();
            BinaryFormatter bf = new BinaryFormatter();
            bf.Serialize(ms, obj);
        }
        catch (Exception ex)
        {
     //      BugReport.ReportLocalError("Compress", typeof(T).ToString(), ex);
        }
        return ms;
    }

    /// <summary>
    /// 压缩
    /// </summary>
    /// <typeparam name="T">压缩类型</typeparam>
    /// <param name="obj">压缩对象</param>
    /// <returns>字节</returns>
    public static T Decompress<T>(byte[] bs)
    {
        T obj = default(T);
        try
        {
            if (bs != null)
            {
                MemoryStream ms = new MemoryStream(bs, false);
                MemoryStream ms2 = new MemoryStream();
                Decompress(ms, ms2);
                obj = (T)new BinaryFormatter().Deserialize(ms2);
            }
        }
        catch (Exception ex)
        {
      //      BugReport.ReportLocalError("Decompress", typeof(T).ToString(), ex);
        }
        return obj;
    }

    /// <summary>
    /// 压缩
    /// </summary>
    /// <typeparam name="T">压缩类型</typeparam>
    /// <param name="obj">压缩对象</param>
    /// <returns>字节</returns>
    public static T Decompress<T>(Stream ms)
    {
        T obj = default(T);
        try
        {
            if (ms != null)
            {
                MemoryStream ms2 = new MemoryStream();
                Decompress(ms, ms2);
                obj = (T)new BinaryFormatter().Deserialize(ms2);
            }
        }
        catch (Exception ex)
        {
    //        BugReport.ReportLocalError("Decompress", typeof(T).ToString(), ex);
        }
        return obj;
    }

    /// <summary>
    /// 压缩
    /// </summary>
    /// <param name="source">源数据流</param>
    /// <param name="dest">目标数据流</param>
    private static void Compress(Stream source, Stream dest)
    {
        using (GZipStream zipStream = new GZipStream(dest, CompressionMode.Compress, true))
        {
            source.Position = 0;//从开始位置开始读取
            int readLength = 1024;
            byte[] buf = new byte[readLength];
            int len = 0;
            while ((len = source.Read(buf, 0, readLength)) > 0)
            {
                zipStream.Write(buf, 0, len);
            }
        }
    }

    /// <summary>
    /// 解压缩
    /// </summary>
    /// <param name="source">源数据流</param>
    /// <param name="dest">目标数据流</param>
    public static void Decompress(Stream source, Stream dest)
    {
        using (GZipStream zipStream = new GZipStream(source, CompressionMode.Decompress, true))
        {
            int readLength = 1024;
            byte[] buf = new byte[readLength];
            int len = 0;
            while ((len = zipStream.Read(buf, 0, readLength)) > 0)
            {
                dest.Write(buf, 0, len);
            }
            dest.Position = 0;//反序列化之前将流位置设为开始
        }
    }
    #endregion
}
