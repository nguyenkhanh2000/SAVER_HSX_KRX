using MDDSCore.Boards.Short;
using Newtonsoft.Json;

namespace MDDSCore.Boards.Full
{
	/// <summary>
	/// 2020-10-02 10:19:27 ngocta2
	/// thong tin chung khoan rut gon, cho bang gia khop lenh, thoa thuan, lo le
	/// </summary>	
	public class BSQuote : BBase
	{
		//public string SymbolID { get; set; }
		public string s { get; set; }
		public string m { get; set; }
		public string b { get; set; }
		public double r { get; set; }
		public double c { get; set; }
		public double f { get; set; }
		public string bc { get; set; }
		public string tbq { get; set; }
		public double bpx { get; set; }
		public long bqx { get; set; }
		public double bp9 { get; set; }
		public long bq9 { get; set; }
		public double bp8 { get; set; }
		public long bq8 { get; set; }
		public double bp7 { get; set; }
		public long bq7 { get; set; }
		public double bp6 { get; set; }
		public long bq6 { get; set; }
		public double bp5 { get; set; }
		public long bq5 { get; set; }
		public double bp4 { get; set; }
		public long bq4 { get; set; }
		public double bp3 { get; set; }
		public long bq3 { get; set; }
		public double bp2 { get; set; }
		public long bq2 { get; set; }
		public double bp1 { get; set; }
		public long bq1 { get; set; }
		public double mp { get; set; }
		public long mq { get; set; }
		public double mc { get; set; }
		public double mcp { get; set; }
		public long tnq { get; set; }
		public double sp1 { get; set; }
		public long sq1 { get; set; }
		public double sp2 { get; set; }
		public long sq2 { get; set; }
		public double sp3 { get; set; }
		public long sq3 { get; set; }
		public double sp4 { get; set; }
		public long sq4 { get; set; }
		public double sp5 { get; set; }
		public long sq5 { get; set; }
		public double sp6 { get; set; }
		public long sq6 { get; set; }
		public double sp7 { get; set; }
		public long sq7 { get; set; }
		public double sp8 { get; set; }
		public long sq8 { get; set; }
		public double sp9 { get; set; }
		public long sq9 { get; set; }
		public double spx { get; set; }
		public long sqx { get; set; }
		public long oc { get; set; }
		public long toq { get; set; }
		public double op { get; set; }
		public double av { get; set; }
		public double hi { get; set; }
		public double lo { get; set; }
		public long fbq { get; set; }
		public long fsq { get; set; }
		public long frr { get; set; }
		public double oi { get; set; }
		public string ltd { get; set; }
		public string ect { get; set; }
		public string stm { get; set; }
		public string sts { get; set; }
		public double tep { get; set; }
		public double etp { get; set; }
		public long etq { get; set; }

		// ===============================================================

		public BSQuote() { }
		public BSQuote(BFQuote entity)
		{
			//this.SymbolID = entity.SymbolID;
			this.s        = entity.Symbol;
			this.m        = entity.MarketID;
			this.b        = entity.BoardID;
			this.r        = entity.Reference;
			this.c        = entity.Ceiling;
			this.f        = entity.Floor;
			this.bc       = entity.BidCount;
			this.tbq      = entity.TotalBidQtty;
			this.bpx      = entity.BuyPriceX;
			this.bqx      = entity.BuyQuantityX;
			this.bp9      = entity.BuyPrice9;
			this.bq9      = entity.BuyQuantity9;
			this.bp8      = entity.BuyPrice8;
			this.bq8      = entity.BuyQuantity8;
			this.bp7      = entity.BuyPrice7;
			this.bq7      = entity.BuyQuantity7;
			this.bp6      = entity.BuyPrice6;
			this.bq6      = entity.BuyQuantity6;
			this.bp5      = entity.BuyPrice5;
			this.bq5      = entity.BuyQuantity5;
			this.bp4      = entity.BuyPrice4;
			this.bq4      = entity.BuyQuantity4;
			this.bp3      = entity.BuyPrice3;
			this.bq3      = entity.BuyQuantity3;
			this.bp2      = entity.BuyPrice2;
			this.bq2      = entity.BuyQuantity2;
			this.bp1      = entity.BuyPrice1;
			this.bq1      = entity.BuyQuantity1;
			this.mp       = entity.MatchPrice;
			this.mq       = entity.MatchQuantity;
			this.mc       = entity.MatchChange;
			this.mcp      = entity.MatchChangePercent;
			this.tnq      = entity.TotalNMQuantity;
			this.sp1      = entity.SellPrice1;
			this.sq1      = entity.SellQuantity1;
			this.sp2      = entity.SellPrice2;
			this.sq2      = entity.SellQuantity2;
			this.sp3      = entity.SellPrice3;
			this.sq3      = entity.SellQuantity3;
			this.sp4      = entity.SellPrice4;
			this.sq4      = entity.SellQuantity4;
			this.sp5      = entity.SellPrice5;
			this.sq5      = entity.SellQuantity5;
			this.sp6      = entity.SellPrice6;
			this.sq6      = entity.SellQuantity6;
			this.sp7      = entity.SellPrice7;
			this.sq7      = entity.SellQuantity7;
			this.sp8      = entity.SellPrice8;
			this.sq8      = entity.SellQuantity8;
			this.sp9      = entity.SellPrice9;
			this.sq9      = entity.SellQuantity9;
			this.spx      = entity.SellPriceX;
			this.sqx      = entity.SellQuantityX;
			this.oc       = entity.OfferCount;
			this.toq      = entity.TotalOfferQtty;
			this.op       = entity.OpenPrice;
			this.av       = entity.AveragePrice;
			this.hi       = entity.HighestPrice;
			this.lo       = entity.LowestPrice;
			this.fbq      = entity.ForeignBuyQuantity;
			this.fsq      = entity.ForeignSellQuantity;
			this.frr      = entity.ForeignRoomRemain;
			this.oi       = entity.OpenInterest;
			this.ltd      = entity.LastTradingDate;
			this.ect      = entity.ExClassType;
			this.stm      = entity.SymbolTradingMethod;
			this.sts      = entity.SymbolTradingSanction;
			this.tep      = entity.TentativeExecutionPrice;
			this.etp      = entity.ExpectedTradePx;
			this.etq      = entity.ExpectedTradeQty;
		}
	}
}
