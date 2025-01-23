using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
    /// <summary>
    /// cac redis key luu trong 1 file json rieng RedisKeys.json
    /// RedisKeys.json cua eztrade co the dinh nghia ten key khac RedisKeys.json cua ezfutures
    /// </summary>
    public class ERedisKeys
    {
        // test
        public const string __KEY_TEST_STRING           = "INIT:S6G_TEST_STRING";
        public const string __KEY_TEST_JSON             = "INIT:S6G_TEST_JSON";
        public const string __KEY_TEST_ZSET             = "INIT:S6G_TEST_ZSET";
        //public const string __KEY_TEMPLATE_MARKETSTATUS = "MARKETSTATUS:S6G_<Exchange>";
        //public const string __KEY_TEMPLATE_BASKET       = "BASKET:S6G_<Exchange>";

        // ten redis key cu the (hoac template redis key) KHONG DE TAI DAY
        // sung dung file config rieng >>> D:\Source\Repos\Stock6G\RootApiService\RedisKeys.json

        // const
        public const string __FILENAME               = "RedisKeys.json";
        public const string __SECTION_REDISKEYS      = "RedisKeys";
        public const string __SECTION_ROOTAPISERVICE = "RootApiService";
        public const string __SECTION_HSXAPISERVICE  = "HsxApiService";
        public const string __SECTION_HNXAPISERVICE  = "HnxApiService";
        public const string __SECTION_HSXFEEDERAPP   = "HsxFeederApp";
        public const string __SECTION_HNXFEEDERAPP   = "HnxFeederApp";
        public const string __TEMPLATE_OBJECT_PUBLIC = "{'Text':'(Text)','Code':'(Code)'}";

        // sub class
        public class ERootApiService
        {
            /// <summary>
            /// <para><i>string type</i></para>
            /// <para><b>BASKET:S6G_COMPANY</b></para>   
            /// </summary>
            public string Company { get; set; }
            public string Ministry { get; set; }
            public string MenuBar { get; set; }
            public string IntradayDetail { get; set; }
            public string Config { get; set; }
            public string CW { get; set; }
        }

        public class ECommonKeys
        {
            /// <summary>
            /// "TradeDate": "TRADEDATE:S6G_HSX",
            /// </summary>
            public string TradeDate { get; set; }
            /// <summary>
            /// "MarketStatus": "MARKETSTATUS:S6G_HSX",
            /// </summary>
            public string MarketStatus { get; set; }
            /// <summary>
            /// "Basket": "BASKET:S6G_HSX",
            /// </summary>
            public string Basket { get; set; }
            /// <summary>
            /// "Basic": "BASIC:S6G_HSX",
            /// </summary>
            public string Basic { get; set; }
            /// <summary>
            /// "LastIndex": "INIT:S6G_LAST_INDEX_HSX",
            /// </summary>
            public string LastIndex { get; set; }
            /// <summary>
            /// "FullQuote": "FULLQUOTE:S6G_FQ_HSX_{MarketID}_{BoardID}",
            /// </summary>
            public string FullQuote { get; set; }
            /// <summary>
            /// "FullIndex": "FULLINDEX:S6G_FI_HSX"
            /// </summary>
            public string FullIndex { get; set; }
            /// <summary>
            /// "RamDicQuote": "RAM:S6G_DIC_QUOTE_HSX",
            /// </summary>
            public string RamDicQuote { get; set; }
            /// <summary>
            /// "RamDicIndex": "RAM:S6G_DIC_INDEX_HSX"
            /// </summary>
            public string RamDicIndex { get; set; }
            
        }

        /// <summary>
        /// luu key redis cua HSX hoac HNX tuy vao config tro vao block nao trong D:\Source\Repos\Stock6G\HSXApiService\RedisKeys.json
        /// </summary>
        public class ERealTimeApiService : ECommonKeys { }
        public class EHsxApiService : ERealTimeApiService { }
        public class EHnxApiService : ERealTimeApiService { }

        public class ERealTimeFeederApp : ECommonKeys { }
        public class EHsxFeederApp : ERealTimeFeederApp { }
        public class EHnxFeederApp : ERealTimeFeederApp { }

        // ---------------------------------------------------------

        /// <summary>
        /// ten phai giong het trong file config
        /// viet ngan gon RootApiService thanh RAS la ko nhan ra
        /// </summary>
        public ERootApiService RootApiService { get; set; }
        public EHsxApiService HsxApiService { get; set; }
        public EHnxApiService HnxApiService { get; set; }
        public EHsxFeederApp HsxFeederApp { get; set; }
        public EHnxFeederApp HnxFeederApp { get; set; }

        ///// <summary>
        ///// <para><i>string type</i></para>
        ///// <para><b>BASKET:S6G_BASKET_HSX</b></para>   
        ///// </summary>
        //public string BasketHsx { get; set; }

        ///// <summary>
        ///// <para><i>string type</i></para>
        ///// <para><b>BASKET:S6G_BASKET_HNX</b></para>   
        ///// </summary>
        //public string BasketHnx { get; set; }

        public ERedisKeys()
        {
            this.RootApiService = new ERootApiService();
            this.HsxApiService = new EHsxApiService();
            this.HnxApiService = new EHnxApiService();
            this.HsxFeederApp = new EHsxFeederApp();
            this.HnxFeederApp = new EHnxFeederApp();
        }
    }
}
