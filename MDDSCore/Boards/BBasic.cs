using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace MDDSCore.Boards
{
	/// <summary>
	/// 2020-10-01 15:48:20 ngocta2
	/// thong tin co ban cua chung khoan
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class BBasic : BBase
	{
		/// <summary>
		/// MarketID - m
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_MARKETID)]
		[DataMember(Name = __SHORT_MARKETID)]
		public string MarketID { get; set; }

		/// <summary>
		/// BoardID - b
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BOARDID)]
		[DataMember(Name = __SHORT_BOARDID)]
		public string BoardID { get; set; }

		/// <summary>
		/// Symbol - s
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SYMBOL)]
		[DataMember(Name = __SHORT_SYMBOL)]
		public string Symbol { get; set; }

		/// <summary>
		/// ReferencePrice - r
		/// </summary>		
		[JsonProperty(PropertyName = __SHORT_REFERENCEPRICE)]
		[DataMember(Name = __SHORT_REFERENCEPRICE)]
		public double? ReferencePrice { get; set; }

		/// <summary>
		/// CeilingPrice - c
		/// </summary>		
		[JsonProperty(PropertyName = __SHORT_CEILINGPRICE)]
		[DataMember(Name = __SHORT_CEILINGPRICE)]
		public double? CeilingPrice { get; set; }

		/// <summary>
		/// FloorPrice - f
		/// </summary>		
		[JsonProperty(PropertyName = __SHORT_FLOORPRICE)]
		[DataMember(Name = __SHORT_FLOORPRICE)]
		public double? FloorPrice { get; set; }
	}
}
