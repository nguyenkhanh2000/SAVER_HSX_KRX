using System;
using System.Collections.Generic;
using System.Text;

namespace BaseSaverLib
{
	public class ESaverConfig
	{
		public const string __SECTION_SAVER_CONFIG = "SaverConfig";

		public string PriceServiceUrl { get; set; }
        public int BatchSize { get; set; }
        public int TIME_DELAY { get; set; }
		public int TIMER_PROCESS_DATA_REDIS { get; set; }
		public int TIMER_PROCESS_DATA_DATABASE {  get; set; }	
    }
}
