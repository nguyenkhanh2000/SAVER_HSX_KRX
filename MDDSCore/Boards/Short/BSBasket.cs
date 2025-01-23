using MDDSCore.Boards.Short;
using Newtonsoft.Json;

namespace MDDSCore.Boards.Full
{
	/// <summary>
	/// 2020-10-01 15:48:20 ngocta2
	/// thong tin ro chung khoan
	/// </summary>	
	public class BSBasket : BBase
	{
		/// <summary>
		/// MarketID - m
		/// </summary>		
		public string m { get; set; }

		/// <summary>
		/// Name - n
		/// </summary>		
		public string n { get; set; }

		/// <summary>
		/// List - l
		/// </summary>		
		public string l { get; set; }

		// ===============================================================

		public BSBasket() { }
		public BSBasket(BFBasket entity)
		{
			this.m = entity.MarketID;
			this.n = entity.Name;
			this.l = entity.List;
		}
	}
}
