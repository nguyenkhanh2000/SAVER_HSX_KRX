using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
    public class ERedisConfigs
    {
        public const string __SECTION_REDIS_CONFIG = "BrokerConfig:RedisConfig";
        /// <summary>
        /// "0.0.0.0"
        /// "fit-ngocta2"
        /// </summary>
        public string Host { get; set; }         /// <summary>
                                                         /// port 5672 thông thì được
                                                         /// </summary>
        public int Port { get; set; }
    }
}
