using MDDSCore.Boards.Full;
using Newtonsoft.Json;

namespace MDDSCore.Boards.Short
{
	/// <summary>
	/// 2020-10-01 15:48:20 ngocta2
	/// tinh trang thi truong
	/// </summary>
	public class BSMarketStatus : BBase
	{
		/// <summary>
		/// MarketID - m
		/// </summary>		
		public string m { get; set; }

		/// <summary>
		/// BoardID - b
		/// </summary>		
		public string b { get; set; }

		/// <summary>
		/// BoardEvtID - be
		/// </summary>		
		public string be { get; set; }

		/// <summary>
		/// TradingSessionID - ts
		/// </summary>
		public string ts { get; set; }

		// ===============================================

		public BSMarketStatus() { }
		public BSMarketStatus(BFMarketStatus entity)
		{
			this.m  = entity.MarketID;
			this.b  = entity.BoardID;
			this.be = entity.BoardEvtID;
			this.ts = entity.TradingSessionID;
		}
	}
}
