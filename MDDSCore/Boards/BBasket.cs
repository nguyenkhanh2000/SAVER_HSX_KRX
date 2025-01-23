using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace MDDSCore.Boards
{
	/// <summary>
	/// 2020-10-01 15:48:20 ngocta2
	/// thong tin ro chung khoan
	/// </summary>	
	[JsonObject(MemberSerialization.OptIn)]
	public class BBasket : BBase
	{
		/// <summary>
		/// MarketID - m
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_MARKETID)]
		[DataMember(Name = __SHORT_MARKETID)]
		public string MarketID { get; set; }

		/// <summary>
		/// Name - n
		/// </summary>	
		[JsonProperty(PropertyName = __SHORT_NAME)]
		[DataMember(Name = __SHORT_NAME)]
		public string Name { get; set; }

		/// <summary>
		/// List - l
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_LIST)]
		[DataMember(Name = __SHORT_LIST)]
		public string List { get; set; }
			
	}
}
