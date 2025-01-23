using MDDSCore.Boards.Short;
using Newtonsoft.Json;

namespace MDDSCore.Boards.Full
{
	/// <summary>
	/// 2020-10-01 15:48:20 ngocta2
	/// thong tin co ban cua chung khoan
	/// </summary>	
	public class BSBasic : BBase
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
		/// Symbol - s
		/// </summary>		
		public string s { get; set; }

		/// <summary>
		/// ReferencePrice - r
		/// </summary>				
		public double r { get; set; }

		/// <summary>
		/// CeilingPrice - c
		/// </summary>				
		public double c { get; set; }

		/// <summary>
		/// FloorPrice - f
		/// </summary>				
		public double f { get; set; }

		// ===============================================================

		public BSBasic() { }
		public BSBasic(BFBasic entity)
		{
			this.m = entity.MarketID;
			this.b = entity.BoardID;
			this.s = entity.Symbol;
			this.r = entity.ReferencePrice;
			this.c = entity.CeilingPrice;
			this.f = entity.FloorPrice;
		}
	}
}
