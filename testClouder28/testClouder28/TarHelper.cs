using ICSharpCode.SharpZipLib.Tar;
using System;
using System.IO;

public class TarHelper
{
	public TarHelper(){
	}

    // <summary>  
    /// tar包解压  
    /// </summary>  
    /// <param name="strFilePath">tar包路径</param>  
    /// <param name="strUnpackDir">解压到的目录</param>  
    /// <returns></returns>  
    public static bool UnpackTarFiles(FileStream fr, string strUnpackDir)
    {
        try
        {
            strUnpackDir = strUnpackDir.Replace("/", "\\");
            if (!strUnpackDir.EndsWith("\\"))
            {
                strUnpackDir += "\\";
            }

            if (!Directory.Exists(strUnpackDir))
            {
                Directory.CreateDirectory(strUnpackDir);
            }

            TarInputStream s = new TarInputStream(fr);
            TarEntry theEntry;
            while ((theEntry = s.GetNextEntry()) != null)
            {
                string directoryName = Path.GetDirectoryName(theEntry.Name);
                string fileName = Path.GetFileName(theEntry.Name);

                if (directoryName != String.Empty)
                    Directory.CreateDirectory(strUnpackDir + directoryName);

                if (fileName != String.Empty)
                {
                    FileStream streamWriter = File.Create(strUnpackDir + theEntry.Name);

                    int size = 2048;
                    byte[] data = new byte[2048];
                    while (true)
                    {
                        size = s.Read(data, 0, data.Length);
                        if (size > 0)
                        {
                            streamWriter.Write(data, 0, size);
                        }
                        else
                        {
                            break;
                        }
                    }

                    streamWriter.Close();
                }
            }
            s.Close();
            fr.Close();

            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }


    // <summary>  
    /// tar包解压  
    /// </summary>  
    /// <param name="strFilePath">tar包路径</param>  
    /// <param name="strUnpackDir">解压到的目录</param>  
    /// <returns></returns>  
    public static bool UnpackTarFiles(MemoryStream mr, string strUnpackDir)
    {
        try
        {
            strUnpackDir = strUnpackDir.Replace("/", "\\");
            if (!strUnpackDir.EndsWith("\\"))
            {
                strUnpackDir += "\\";
            }

            if (!Directory.Exists(strUnpackDir))
            {
                Directory.CreateDirectory(strUnpackDir);
            }

            TarInputStream s = new TarInputStream(mr);
            TarEntry theEntry;
            while ((theEntry = s.GetNextEntry()) != null)
            {
                string directoryName = Path.GetDirectoryName(theEntry.Name);
                string fileName = Path.GetFileName(theEntry.Name);

                if (directoryName != String.Empty)
                    Directory.CreateDirectory(strUnpackDir + directoryName);

                if (fileName != String.Empty)
                {
                    FileStream streamWriter = File.Create(strUnpackDir + theEntry.Name);

                    int size = 2048;
                    byte[] data = new byte[2048];
                    while (true)
                    {
                        size = s.Read(data, 0, data.Length);
                        if (size > 0)
                        {
                            streamWriter.Write(data, 0, size);
                        }
                        else
                        {
                            break;
                        }
                    }

                    streamWriter.Close();
                }
            }
            s.Close();
            mr.Close();

            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }



    // <summary>  
    /// tar包解压  
    /// </summary>  
    /// <param name="strFilePath">tar包路径</param>  
    /// <param name="strUnpackDir">解压到的目录</param>  
    /// <returns></returns>  
    public static bool UnpackTarFiles(string strFilePath, string strUnpackDir)
    {
        try
        {
            if (!File.Exists(strFilePath))
            {
                return false;
            }

            strUnpackDir = strUnpackDir.Replace("/", "\\");
            if (!strUnpackDir.EndsWith("\\"))
            {
                strUnpackDir += "\\";
            }

            if (!Directory.Exists(strUnpackDir))
            {
                Directory.CreateDirectory(strUnpackDir);
            }

            FileStream fr = new FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            TarInputStream s = new TarInputStream(fr);
            TarEntry theEntry;
            while ((theEntry = s.GetNextEntry()) != null)
            {
                string directoryName = Path.GetDirectoryName(theEntry.Name);
                string fileName = Path.GetFileName(theEntry.Name);

                if (directoryName != String.Empty)
                    Directory.CreateDirectory(strUnpackDir + directoryName);

                if (fileName != String.Empty)
                {
                    FileStream streamWriter = File.Create(strUnpackDir + theEntry.Name);

                    int size = 2048;
                    byte[] data = new byte[2048];
                    while (true)
                    {
                        size = s.Read(data, 0, data.Length);
                        if (size > 0)
                        {
                            streamWriter.Write(data, 0, size);
                        }
                        else
                        {
                            break;
                        }
                    }

                    streamWriter.Close();
                }
            }
            s.Close();
            fr.Close();

            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
