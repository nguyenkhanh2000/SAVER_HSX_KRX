using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
    public class EKafkaLoggerConfig
    {

        public const string __SECTION_KAFKALOGGERCONFIG = "KafkaLoggerConfig";


        /// <summary>
        /// 10.26.7.58:9092,10.26.7.59:9092,10.26.7.60:9092
        /// </summary>
        public string BootstrapServers { get; set; }

        /// <summary>
        /// log-app
        /// </summary>
        public string Topic { get; set; }


        /// <summary>
        /// Stock6G
        /// </summary>
        public string Group { get; set; }


        /// <summary>
        /// FIT-BDRD
        /// </summary>
        public string Department { get; set; }

    }
}
