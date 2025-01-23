using MDDSCore.Boards.Short;
using Newtonsoft.Json;

namespace MDDSCore.Boards.Full
{
	/// <summary>
	/// 2020-10-01 15:48:20 ngocta2
	/// tinh trang thi truong
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class BFMarketStatus: BBase
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
		/// BoardEvtID - be
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BOARDEVTID)]
		public string BoardEvtID { get; set; }

		/// <summary>
		/// TradingSessionID - ts
		/// </summary>		
		[JsonProperty(PropertyName = __SHORT_TRADINGSESSIONID)]
		public string TradingSessionID { get; set; }

		// ===============================================

		public BFMarketStatus() { }
		public BFMarketStatus (BSMarketStatus entity)
		{
			this.MarketID         = entity.m;
			this.BoardID          = entity.b;
			this.BoardEvtID       = entity.be;
			this.TradingSessionID = entity.ts;
		}
	}
}
