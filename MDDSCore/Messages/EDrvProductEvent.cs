using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
namespace MDDSCore.Messages
{
    /// <summary>
	/// 2024-26-01  
	/// <para><i>SPEC version=; date=2024.01.20</i></para>
	/// <para><i>; name=DrvProductEvent; type=MJ; Batch/Online</i></para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
    public class EDrvProductEvent : EBase
    {
        /// <summary>
        /// MJ = DRV_PRODUCT_EVENT
        /// </summary>
        public const string __MSG_TYPE = __MSG_TYPE_DRV_PRODUCT_EVENT;
        [JsonProperty(PropertyName = __TAG_20009, Order = 8)]
        public string ProductID { get; set; }
        [JsonProperty(PropertyName = __TAG_30575, Order = 8)]
        public string EventKindCode { get; set; }
        [JsonProperty(PropertyName = __TAG_30576, Order = 8)]
        public string EventOccurrenceReasonCode { get; set; }
        [JsonProperty(PropertyName = __TAG_30577, Order = 8)]
        public string EventStartDate { get; set; }
        [JsonProperty(PropertyName = __TAG_30578, Order = 8)]
        public string EventEndDate { get; set; }

    }
}
