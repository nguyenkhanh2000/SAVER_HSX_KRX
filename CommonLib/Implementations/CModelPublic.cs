using Newtonsoft.Json;
using StockCore.Redis.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace CommonLib.Implementations
{
    public class CModelPublic
    {
        public class IG_BI_FULL_NEW
        {
            [JsonProperty(PropertyName = "8", Order = 1)]
            public string BeginString { get; set; }

            [JsonProperty(PropertyName = "9", Order = 2)]
            public string BodyLength { get; set; }

            [JsonProperty(PropertyName = "35", Order = 3)]
            public string MsgType { get; set; }

            [JsonProperty(PropertyName = "49", Order = 4)]
            public string SenderCompID { get; set; }

            [JsonProperty(PropertyName = "52", Order = 5)]
            public string SendingTime { get; set; }

            [JsonProperty(PropertyName = "421", Order = 6)]
            public string Name { get; set; }

            [JsonProperty(PropertyName = "422", Order = 7)]
            public string Shortname { get; set; }

            [JsonProperty(PropertyName = "251", Order = 8)]
            public string numSymbolAdvances { get; set; }

            [JsonProperty(PropertyName = "253", Order = 9)]
            public string numSymbolDeclines { get; set; }

            [JsonProperty(PropertyName = "252", Order = 10)]
            public string numSymbolNochange { get; set; }

            [JsonProperty(PropertyName = "210", Order = 11)]
            public string totalNormalTradedQttyOd { get; set; }

            [JsonProperty(PropertyName = "211", Order = 12)]
            public string totalNormalTradedValueOd { get; set; }

            [JsonProperty(PropertyName = "220", Order = 13)]
            public string totalNormalTradedQttyRd { get; set; }

            [JsonProperty(PropertyName = "221", Order = 14)]
            public string totalNormalTradedValueRd { get; set; }

            [JsonProperty(PropertyName = "240", Order = 15)]
            public string totalPTTradedQtty { get; set; }

            [JsonProperty(PropertyName = "241", Order = 16)]
            public string totalPTTradedValue { get; set; }

            [JsonProperty(PropertyName = "270", Order = 17)]
            public string TotalTrade { get; set; }

            [JsonProperty(PropertyName = "17", Order = 18)]
            public string DateNo { get; set; }

            [JsonProperty(PropertyName = "425", Order = 19)]
            public string BoardCode { get; set; }

            [JsonProperty(PropertyName = "426", Order = 20)]
            public string BoardStatus { get; set; }

            [JsonProperty(PropertyName = "388", Order = 21)]
            public string Tradingdate { get; set; }

            [JsonProperty(PropertyName = "399", Order = 22)]
            public string Time { get; set; }

            [JsonProperty(PropertyName = "250", Order = 23)]
            public string TotalStock { get; set; }

            [JsonProperty(PropertyName = "336", Order = 24)]
            public string TradingSessionID { get; set; }

            [JsonProperty(PropertyName = "340", Order = 25)]
            public string TradSesStatus { get; set; }

            [JsonProperty(PropertyName = "341", Order = 26)]
            public string f341 { get; set; }
        }
        public class EDataSingle : EBaseValue
        {
            /// <summary>
            /// thoi gian update data vao redis (auto)
            /// </summary>
            [JsonProperty(Order = 1)]
            [DataMember(Name = __FULL_TIME, Order = 1)]
            public string Time { get; set; } = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");

            /// <summary>
            /// noi dung data chinh can luu redis
            /// </summary>
            [JsonProperty(Order = 2)]
            [DataMember(Name = __FULL_DATA, Order = 2)]
            public List<IG_BI_FULL_NEW> Data { get; set; }

            /// <summary>
            /// constructor
            /// </summary>
            public EDataSingle(List<IG_BI_FULL_NEW> data) : base()
            {
                Data = data;
            }
        }
    }
}
