using MDDSCore.Boards.Short;
using Newtonsoft.Json;

namespace MDDSCore.Boards.Full
{
	/// <summary>
	/// 2020-10-01 15:48:20 ngocta2
	/// thong tin co ban cua chung khoan
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class BFBasic : BBase
	{
		/// <summary>
		/// MarketID - m
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_MARKETID)]
		public string MarketID { get; set; }

		/// <summary>
		/// BoardID - b
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BOARDID)]
		public string BoardID { get; set; }

		/// <summary>
		/// Symbol - s
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SYMBOL)]
		public string Symbol { get; set; }

		/// <summary>
		/// ReferencePrice - r
		/// </summary>		
		[JsonProperty(PropertyName = __SHORT_REFERENCEPRICE)]
		public double ReferencePrice { get; set; }

		/// <summary>
		/// CeilingPrice - c
		/// </summary>		
		[JsonProperty(PropertyName = __SHORT_CEILINGPRICE)]
		public double CeilingPrice { get; set; }

		/// <summary>
		/// FloorPrice - f
		/// </summary>		
		[JsonProperty(PropertyName = __SHORT_FLOORPRICE)]
		public double FloorPrice { get; set; }

		// ===============================================================

		public BFBasic() { }
		public BFBasic(BSBasic entity)
		{
			this.MarketID       = entity.m;
			this.BoardID        = entity.b;			
			this.Symbol         = entity.s;
			this.ReferencePrice = entity.r;
			this.CeilingPrice   = entity.c;
			this.FloorPrice     = entity.f;
		}
	}
}
