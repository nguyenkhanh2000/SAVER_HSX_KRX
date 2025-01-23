using System;
using System.Collections.Generic;
using System.Text;

namespace BaseBrokerLib
{
    /// <summary>
    /// 2020-07-14 14:39:46 ngocta2
    /// </summary>
	public class EBrokerOutConfig
    {
        public enum BrokerConfigList
        {
            None = 0,
            Input = 1,
            Output = 2
        }
        public const string __SECTION_BROKER_CONFIG = "BrokerConfig";
        public const string __SECTION_BROKER_INPUT_CONFIG = "BrokerConfig:Input";
        public const string __SECTION_BROKER_OUTPUT_CONFIG = "BrokerConfig:Output";
        public const string __SECTION_BROKER_OUTPUT_INDEX_CONFIG = "BrokerConfig:OutputIndex";
        public const string __SECTION_BROKER_OUTPUT_ET_CONFIG = "BrokerConfig:OutputET";

        public const string __SECTION_BROKER_OUTPUT_G4_CONFIG = "BrokerConfig:OutputG4";
        public const string __SECTION_BROKER_OUTPUT_G7_CONFIG = "BrokerConfig:OutputG7";
        public const string __SECTION_BROKER_OUTPUT_G8_CONFIG = "BrokerConfig:OutputG8";
        public const string __SECTION_BROKER_OUTPUT_T1_CONFIG = "BrokerConfig:OutputT1";
        public const string __SECTION_BROKER_OUTPUT_T2_CONFIG = "BrokerConfig:OutputT2";
        public const string __SECTION_BROKER_OUTPUT_T3_CONFIG = "BrokerConfig:OutputT3";
        public const string __SECTION_BROKER_OUTPUT_T4_CONFIG = "BrokerConfig:OutputT4";
        public const string __SECTION_BROKER_OUTPUT_T6_CONFIG = "BrokerConfig:OutputT6";
        public const string __SECTION_BROKER_OUTPUT_T7_CONFIG = "BrokerConfig:OutputT7";
        //public const string __SECTION_BROKER_OUTPUT_INDEX_HNX_CONFIG = "BrokerConfig:OutputIndexHNX";

        /*
         * rabbitmq
            string rabbitHostName = config.GetSection("RABBIT:Fix2CoreDB:HostName")?.Value; // ip máy đặt Queue
            int    rabbitPort     = int.Parse(config.GetSection("RABBIT:Fix2CoreDB:Port")?.Value); // user : agent
            string userName       = config.GetSection("RABBIT:Fix2CoreDB:UserName")?.Value; // pass : agent
            string passWord       = config.GetSection("RABBIT:Fix2CoreDB:Password")?.Value; //5672
            string queueName      = config.GetSection("RABBIT:Fix2CoreDB:QueueName")?.Value; // tên Queue
            string queueEXChange  = config.GetSection("RABBIT:Fix2CoreDB:ExchangeName")?.Value;
            string queueRouting   = config.GetSection("RABBIT:Fix2CoreDB:RoutingKey")?.Value;
            bool   rbQueueDurable = Convert.ToBoolean(config.GetSection("RABBIT:Fix2CoreDB:Durable")?.Value);
         */

        /// <summary>
        /// "0.0.0.0"
        /// "fit-ngocta2"
        /// </summary>
        public string Host { get; set; }

        /// <summary>
        /// port 5672 thông thì được
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// "ngocta2"
        /// </summary>
        public string Username { get; set; }

        /// <summary>
        /// "ngoc123456"
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// "QueueName"
        /// </summary>
        public string QueueName { get; set; }

        /// <summary>
        /// "ExchangeName"
        /// </summary>
        public string ExchangeName { get; set; }

        /// <summary>
        /// "RoutingKey"
        /// </summary>
        public string RoutingKey { get; set; }

        /// <summary>
        /// true/false
        /// </summary>
        public string Durable { get; set; }

    }
}
