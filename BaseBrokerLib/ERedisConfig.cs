using System;
using System.Collections.Generic;
using System.Text;

namespace BaseBrokerLib
{
    /// <summary>
    /// 2020-07-14 14:39:46 ngocta2
    /// </summary>
	public class ERedisConfig
    {

        public const string REDIS_CONFIG = "RedisConfig";

        /// <summary>
        /// "0.0.0.0"
        /// "fit-ngocta2"
        /// </summary>
        public string Host { get; set; }
        /// <summary>
        /// port 5672 thông thì được
        /// </summary>
        public int Port { get; set; }

        public string HostFOX { get; set; }
        /// <summary>
        /// port 5672 thông thì được
        /// </summary>
        public int PortFOX { get; set; }
        public string HostLLQ { get; set; }
        public int PortLLQ { get; set; }


        public string m_strTemplateSqlElement { get; set; }

        public string m_strTemplateSqlFull { get; set; }
        public string db68Connect { get; set; }

        public string spselectdataET { get; set; }

        public string KeyETHnxQuoTe { get; set; }

        public string m_strTemplateSQLSEQSTX { get; set; }
        public string m_strTemplateSQLSEQUPX { get; set; }
        public string m_strTemplateSQLSEQDVX { get; set; }

        public string spselectdataseqstx { get; set; }
        public string spselectdatasequpx { get; set; }
        public string spselectdataseqdvx { get; set; }

        public string sptruncatetableseq { get; set; }

        public string CHANNEL_S5G_HO_GROUP_INDEX { get; set; }
        public string RedisKeyVNI { get; set; }
        public string RedisKeyVN30 { get; set; }
        public string RedisKeyVN100 { get; set; }
        public string RedisKeyVNALL { get; set; }
        public string RedisKeyVNMID { get; set; }

        public string RedisKeyVNSML { get; set; }
        public string RedisKeyVNXALL { get; set; }
        public string RedisKeyFullRow { get; set; }
        public string CHANNEL_MONITOR_5G { get; set; }

        public string CHANNEL_S5G_HO_GROUP_PT { get; set; }
    }
}
