using System;
using System.Collections.Generic;
using System.Text;

namespace PriceLib
{
    public class CRedisConfig
    {
        public const string __SECTION_PRICECONFIG = "CRedisConfig";

        public string Endpoints_1 { get; set; }
        public string Endpoints_2 { get; set; }
        public int Redis_DB { get; set; }
        public string Host_FOX { get; set; }

        public int Port_FOX { get; set; }
        public string Host_LLQ { get; set; }

        public int Port_LLQ { get; set; }

        public int TIMER_PROCESS_DATA_REDIS { get; set; }

        public string Src_QueueWindown { get; set; }

    }
}
