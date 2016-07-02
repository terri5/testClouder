using ICSharpCode.SharpZipLib.GZip;
using System;
using System.IO;

public class GZipHelper
{
	public GZipHelper()
	{
	}

    public static void UnGzip(Stream src, Stream dest)
    {
        var inStream = new GZipInputStream(src);
        byte[] buf = new byte[100000];
        int currentIndex = 0;
        int count = buf.Length;

        while (true)
        {
            int numRead = inStream.Read(buf, currentIndex, count);
            if (numRead <= 0)
            {
                break;
            }
            dest.Write(buf, 0,numRead);
            currentIndex += numRead;
            count -= numRead;
        }


    }
}
