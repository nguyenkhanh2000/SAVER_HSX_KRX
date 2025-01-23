using MDDSCore.Boards.Short;
using Newtonsoft.Json;

namespace MDDSCore.Boards.Full
{
	/// <summary>
	/// 2020-10-02 08:56:13 ngocta2
	/// quote data
	/// struct day du, mo ta chi tiet
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class BFQuote : BBase
	{
		/// <summary>
		/// giống row id trong table, để map giữa các msg
		/// </summary>
		public string SymbolID { get; set; }

		/// <summary>
		/// Mã chứng khoán
		/// </summary>
		public string Symbol { get; set; }

		/// <summary>
		/// ID xác định các thị trường. VD: STO, BDO, DVX…
		/// </summary>
		public string MarketID { get; set; }

		/// <summary>
		/// ID Bảng giao dịch: G1, G4, T1 ...
		/// </summary>
		public string BoardID { get; set; }

		/// <summary>
		/// giá tham chiếu
		/// </summary>
		public double Reference { get; set; }

		/// <summary>
		/// giá trần
		/// </summary>
		public double Ceiling { get; set; }

		/// <summary>
		/// giá sàn
		/// </summary>
		public double Floor { get; set; }

		/// <summary>
		/// số lệnh mua
		/// </summary>
		public string BidCount { get; set; }

		/// <summary>
		/// tổng KL mua
		/// </summary>
		public string TotalBidQtty { get; set; }

		/// <summary>
		/// giá mua 10
		/// </summary>
		public double BuyPriceX { get; set; }

		/// <summary>
		/// KL mua 10
		/// </summary>
		public long BuyQuantityX { get; set; }

		/// <summary>
		/// giá mua 9
		/// </summary>
		public double BuyPrice9 { get; set; }

		/// <summary>
		/// KL mua 9
		/// </summary>
		public long BuyQuantity9 { get; set; }

		/// <summary>
		/// giá mua 8
		/// </summary>
		public double BuyPrice8 { get; set; }

		/// <summary>
		/// KL mua 8
		/// </summary>
		public long BuyQuantity8 { get; set; }

		/// <summary>
		/// giá mua 7
		/// </summary>
		public double BuyPrice7 { get; set; }

		/// <summary>
		/// KL mua 7
		/// </summary>
		public long BuyQuantity7 { get; set; }

		/// <summary>
		/// giá mua 6
		/// </summary>
		public double BuyPrice6 { get; set; }

		/// <summary>
		/// KL mua 6
		/// </summary>
		public long BuyQuantity6 { get; set; }

		/// <summary>
		/// giá mua 5
		/// </summary>
		public double BuyPrice5 { get; set; }

		/// <summary>
		/// KL mua 5
		/// </summary>
		public long BuyQuantity5 { get; set; }

		/// <summary>
		/// giá mua 4
		/// </summary>
		public double BuyPrice4 { get; set; }

		/// <summary>
		/// KL mua 4
		/// </summary>
		public long BuyQuantity4 { get; set; }

		/// <summary>
		/// giá mua 3
		/// </summary>
		public double BuyPrice3 { get; set; }

		/// <summary>
		/// KL mua 3
		/// </summary>
		public long BuyQuantity3 { get; set; }

		/// <summary>
		/// giá mua 2
		/// </summary>
		public double BuyPrice2 { get; set; }

		/// <summary>
		/// KL mua 2
		/// </summary>
		public long BuyQuantity2 { get; set; }

		/// <summary>
		/// giá mua 1
		/// </summary>
		public double BuyPrice1 { get; set; }

		/// <summary>
		/// KL mua 1
		/// </summary>
		public long BuyQuantity1 { get; set; }

		/// <summary>
		/// giá khớp
		/// </summary>
		public double MatchPrice { get; set; }

		/// <summary>
		/// KL khớp
		/// </summary>
		public long MatchQuantity { get; set; }

		/// <summary>
		/// thay đổi của mp so với giá tham chiếu
		/// </summary>
		public double MatchChange { get; set; }

		/// <summary>
		/// thay đổi % của mp so với giá tham chiếu
		/// </summary>
		public double MatchChangePercent { get; set; }

		/// <summary>
		/// Tổng KL đã khớp (khớp lệnh)
		/// </summary>
		public long TotalNMQuantity { get; set; }

		/// <summary>
		/// giá bán 1
		/// </summary>
		public double SellPrice1 { get; set; }

		/// <summary>
		/// KL bán 1
		/// </summary>
		public long SellQuantity1 { get; set; }

		/// <summary>
		/// giá bán 2
		/// </summary>
		public double SellPrice2 { get; set; }

		/// <summary>
		/// KL bán 2
		/// </summary>
		public long SellQuantity2 { get; set; }

		/// <summary>
		/// giá bán 3
		/// </summary>
		public double SellPrice3 { get; set; }

		/// <summary>
		/// KL bán 3
		/// </summary>
		public long SellQuantity3 { get; set; }

		/// <summary>
		/// giá bán 4
		/// </summary>
		public double SellPrice4 { get; set; }

		/// <summary>
		/// KL bán 4
		/// </summary>
		public long SellQuantity4 { get; set; }

		/// <summary>
		/// giá bán 5
		/// </summary>
		public double SellPrice5 { get; set; }

		/// <summary>
		/// KL bán 5
		/// </summary>
		public long SellQuantity5 { get; set; }

		/// <summary>
		/// giá bán 6
		/// </summary>
		public double SellPrice6 { get; set; }

		/// <summary>
		/// KL bán 6
		/// </summary>
		public long SellQuantity6 { get; set; }

		/// <summary>
		/// giá bán 7
		/// </summary>
		public double SellPrice7 { get; set; }

		/// <summary>
		/// KL bán 7
		/// </summary>
		public long SellQuantity7 { get; set; }

		/// <summary>
		/// giá bán 8
		/// </summary>
		public double SellPrice8 { get; set; }

		/// <summary>
		/// KL bán 8
		/// </summary>
		public long SellQuantity8 { get; set; }

		/// <summary>
		/// giá bán 9
		/// </summary>
		public double SellPrice9 { get; set; }

		/// <summary>
		/// KL bán 9
		/// </summary>
		public long SellQuantity9 { get; set; }

		/// <summary>
		/// giá bán 10
		/// </summary>
		public double SellPriceX { get; set; }

		/// <summary>
		/// KL bán 10
		/// </summary>
		public long SellQuantityX { get; set; }

		/// <summary>
		/// số lệnh bán
		/// </summary>
		public long OfferCount { get; set; }

		/// <summary>
		/// tổng KL bán
		/// </summary>
		public long TotalOfferQtty { get; set; }

		/// <summary>
		/// giá mở cửa
		/// </summary>
		public double OpenPrice { get; set; }

		/// <summary>
		/// giá trung bình
		/// </summary>
		public double AveragePrice { get; set; }

		/// <summary>
		/// giá cao nhất
		/// </summary>
		public double HighestPrice { get; set; }

		/// <summary>
		/// giá thấp nhất
		/// </summary>
		public double LowestPrice { get; set; }

		/// <summary>
		/// KL nước ngoài mua
		/// </summary>
		public long ForeignBuyQuantity { get; set; }

		/// <summary>
		/// KL nước ngoài bán
		/// </summary>
		public long ForeignSellQuantity { get; set; }

		/// <summary>
		/// KL còn lại được phép mua
		/// </summary>
		public long ForeignRoomRemain { get; set; }

		/// <summary>
		/// KL mở
		/// </summary>
		public double OpenInterest { get; set; }

		/// <summary>
		/// ngày đáo hạn
		/// </summary>
		public string LastTradingDate { get; set; }

		/// <summary>
		/// Giao dịch không hưởng quyền
		/// </summary>
		public string ExClassType { get; set; }

		/// <summary>
		/// Ký hiệu để xác định loại giao dịch cho một mã chứng khoán.
		/// </summary>
		public string SymbolTradingMethod { get; set; }

		/// <summary>
		/// "Ký hiệu để phân loại tình trạng giao dịch như tạm ngưng, ngưng giao dịch vì một số lý do."
		/// </summary>
		public string SymbolTradingSanction { get; set; }

		/// <summary>
		/// "Giá dự kiến được thực hiện trong phiên khớp lệnh định kỳ khi RE được áp dụng(Sự kiện RE xảy ra trong các đợt giao dịch khớp lệnh định kỳ.)"
		/// </summary>
		public double TentativeExecutionPrice { get; set; }

		/// <summary>
		/// Giá dự kiến giao dịch (phiên định kỳ khớp lệnh mở cửa)
		/// </summary>
		public double ExpectedTradePx { get; set; }

		/// <summary>
		/// Khối lượng dự kiến giao dịch (phiên định kỳ khớp lệnh mở cửa)
		/// </summary>
		public long ExpectedTradeQty { get; set; }

		// ===============================================================

		public BFQuote() { }
		public BFQuote(BSQuote entity)
		{
			//this.SymbolID              = entity.SymbolID;
			this.Symbol                  = entity.s;
			this.MarketID                = entity.m;
			this.BoardID                 = entity.b;
			this.Reference               = entity.r;
			this.Ceiling                 = entity.c;
			this.Floor                   = entity.f;
			this.BidCount                = entity.bc;
			this.TotalBidQtty            = entity.tbq;
			this.BuyPriceX               = entity.bpx;
			this.BuyQuantityX            = entity.bqx;
			this.BuyPrice9               = entity.bp9;
			this.BuyQuantity9            = entity.bq9;
			this.BuyPrice8               = entity.bp8;
			this.BuyQuantity8            = entity.bq8;
			this.BuyPrice7               = entity.bp7;
			this.BuyQuantity7            = entity.bq7;
			this.BuyPrice6               = entity.bp6;
			this.BuyQuantity6            = entity.bq6;
			this.BuyPrice5               = entity.bp5;
			this.BuyQuantity5            = entity.bq5;
			this.BuyPrice4               = entity.bp4;
			this.BuyQuantity4            = entity.bq4;
			this.BuyPrice3               = entity.bp3;
			this.BuyQuantity3            = entity.bq3;
			this.BuyPrice2               = entity.bp2;
			this.BuyQuantity2            = entity.bq2;
			this.BuyPrice1               = entity.bp1;
			this.BuyQuantity1            = entity.bq1;
			this.MatchPrice              = entity.mp;
			this.MatchQuantity           = entity.mq;
			this.MatchChange             = entity.mc;
			this.MatchChangePercent      = entity.mcp;
			this.TotalNMQuantity         = entity.tnq;
			this.SellPrice1              = entity.sp1;
			this.SellQuantity1           = entity.sq1;
			this.SellPrice2              = entity.sp2;
			this.SellQuantity2           = entity.sq2;
			this.SellPrice3              = entity.sp3;
			this.SellQuantity3           = entity.sq3;
			this.SellPrice4              = entity.sp4;
			this.SellQuantity4           = entity.sq4;
			this.SellPrice5              = entity.sp5;
			this.SellQuantity5           = entity.sq5;
			this.SellPrice6              = entity.sp6;
			this.SellQuantity6           = entity.sq6;
			this.SellPrice7              = entity.sp7;
			this.SellQuantity7           = entity.sq7;
			this.SellPrice8              = entity.sp8;
			this.SellQuantity8           = entity.sq8;
			this.SellPrice9              = entity.sp9;
			this.SellQuantity9           = entity.sq9;
			this.SellPriceX              = entity.spx;
			this.SellQuantityX           = entity.sqx;
			this.OfferCount              = entity.oc;
			this.TotalOfferQtty          = entity.toq;
			this.OpenPrice               = entity.op;
			this.AveragePrice            = entity.av;
			this.HighestPrice            = entity.hi;
			this.LowestPrice             = entity.lo;
			this.ForeignBuyQuantity      = entity.fbq;
			this.ForeignSellQuantity     = entity.fsq;
			this.ForeignRoomRemain       = entity.frr;
			this.OpenInterest            = entity.oi;
			this.LastTradingDate         = entity.ltd;
			this.ExClassType             = entity.ect;
			this.SymbolTradingMethod     = entity.stm;
			this.SymbolTradingSanction   = entity.sts;
			this.TentativeExecutionPrice = entity.tep;
			this.ExpectedTradePx         = entity.etp;
			this.ExpectedTradeQty        = entity.etq;
		}
	}
}
