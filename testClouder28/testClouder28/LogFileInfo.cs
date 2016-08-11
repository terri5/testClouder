namespace testClouder28
{
    public class LogFileInfo
    {
        public const string FILE_TYPE_UV = "uv";
        public const string FILE_TYPE_PV = "pv";
        public const string FILE_TYPE_HIT ="hit";
        public const string PV1_FILE = "localhost.log";
        public const string PV2_FILE = "pv_http.log";
        public const string PV3_FILE = "nginxproxy.log";
        public const string UV_FILE = "uv_time_flow.log";
        public const string HIT_FILE = "hit.log";
        private string fname;
        private string fullname;
        private string dmac;
        private string outputfile;

        public string GetLogType() {
            switch(Fname){
                case PV1_FILE:
                case PV2_FILE:
                case PV3_FILE:
                    return FILE_TYPE_PV;
                case UV_FILE:
                    return FILE_TYPE_UV;
                case HIT_FILE:
                    return FILE_TYPE_HIT;
                default:
                    return null;
            }

        }



        public string Dmac
        {
            get
            {
                return dmac;
            }

            set
            {
                dmac = value;
            }
        }

        public string Fname
        {
            get
            {
                return fname;
            }

            set
            {
                fname = value;
            }
        }

        public string Fullname
        {
            get
            {
                return fullname;
            }

            set
            {
                fullname = value;
            }
        }

        public string Outputfile
        {
            get
            {
                return outputfile;
            }

            set
            {
                outputfile = value;
            }
        }
    }
}