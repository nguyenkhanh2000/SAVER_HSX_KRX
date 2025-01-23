using Newtonsoft.Json;
using System.Runtime.Serialization;

namespace MDDSCore.Boards
{
	/// <summary>
	/// 2020-10-02 08:56:13 ngocta2
	/// quote data
	/// struct day du, mo ta chi tiet
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class BQuote : BBase
	{
		/// <summary>
		/// giống row id trong table, để map giữa các msg
		/// </summary>
		[IgnoreDataMember]
		public string SymbolID { get; set; }

		/// <summary>
		/// Mã chứng khoán
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SYMBOL)]
		[DataMember(Name = __SHORT_SYMBOL)]
		public string Symbol { get; set; }

		/// <summary>
		/// ID xác định các thị trường. VD: STO, BDO, DVX…
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_MARKETID)]
		[DataMember(Name = __SHORT_MARKETID)]
		public string MarketID { get; set; }

		/// <summary>
		/// ID Bảng giao dịch: G1, G4, T1 ...
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BOARDID)]
		[DataMember(Name = __SHORT_BOARDID)]
		public string BoardID { get; set; }

		/// <summary>
		/// giá tham chiếu
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_REFERENCE)]
		[DataMember(Name = __SHORT_REFERENCE)]
		public double? Reference { get; set; }

		/// <summary>
		/// giá trần
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_CEILING)]
		[DataMember(Name = __SHORT_CEILING)]
		public double? Ceiling { get; set; }

		/// <summary>
		/// giá sàn
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_FLOOR)]
		[DataMember(Name = __SHORT_FLOOR)]
		public double? Floor { get; set; }

		/// <summary>
		/// số lệnh mua
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BIDCOUNT)]
		[DataMember(Name = __SHORT_BIDCOUNT)]
		public string BidCount { get; set; }

		/// <summary>
		/// tổng KL mua
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_TOTALBIDQTTY)]
		[DataMember(Name = __SHORT_TOTALBIDQTTY)]
		public string TotalBidQtty { get; set; }

		/// <summary>
		/// giá mua 10
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYPRICEX)]
		[DataMember(Name = __SHORT_BUYPRICEX)]
		public double? BuyPriceX { get; set; }

		/// <summary>
		/// KL mua 10
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYQUANTITYX)]
		[DataMember(Name = __SHORT_BUYQUANTITYX)]
		public long? BuyQuantityX { get; set; }

		/// <summary>
		/// giá mua 9
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYPRICE9)]
		[DataMember(Name = __SHORT_BUYPRICE9)]
		public double? BuyPrice9 { get; set; }

		/// <summary>
		/// KL mua 9
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYQUANTITY9)]
		[DataMember(Name = __SHORT_BUYQUANTITY9)]
		public long? BuyQuantity9 { get; set; }

		/// <summary>
		/// giá mua 8
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYPRICE8)]
		[DataMember(Name = __SHORT_BUYPRICE8)]
		public double? BuyPrice8 { get; set; }

		/// <summary>
		/// KL mua 8
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYQUANTITY8)]
		[DataMember(Name = __SHORT_BUYQUANTITY8)]
		public long? BuyQuantity8 { get; set; }

		/// <summary>
		/// giá mua 7
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYPRICE7)]
		[DataMember(Name = __SHORT_BUYPRICE7)]
		public double? BuyPrice7 { get; set; }

		/// <summary>
		/// KL mua 7
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYQUANTITY7)]
		[DataMember(Name = __SHORT_BUYQUANTITY7)]
		public long? BuyQuantity7 { get; set; }

		/// <summary>
		/// giá mua 6
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYPRICE6)]
		[DataMember(Name = __SHORT_BUYPRICE6)]
		public double? BuyPrice6 { get; set; }

		/// <summary>
		/// KL mua 6
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYQUANTITY6)]
		[DataMember(Name = __SHORT_BUYQUANTITY6)]
		public long? BuyQuantity6 { get; set; }

		/// <summary>
		/// giá mua 5
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYPRICE5)]
		[DataMember(Name = __SHORT_BUYPRICE5)]
		public double? BuyPrice5 { get; set; }

		/// <summary>
		/// KL mua 5
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYQUANTITY5)]
		[DataMember(Name = __SHORT_BUYQUANTITY5)]
		public long? BuyQuantity5 { get; set; }

		/// <summary>
		/// giá mua 4
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYPRICE4)]
		[DataMember(Name = __SHORT_BUYPRICE4)]
		public double? BuyPrice4 { get; set; }

		/// <summary>
		/// KL mua 4
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYQUANTITY4)]
		[DataMember(Name = __SHORT_BUYQUANTITY4)]
		public long? BuyQuantity4 { get; set; }

		/// <summary>
		/// giá mua 3
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYPRICE3)]
		[DataMember(Name = __SHORT_BUYPRICE3)]
		public double? BuyPrice3 { get; set; }

		/// <summary>
		/// KL mua 3
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYQUANTITY3)]
		[DataMember(Name = __SHORT_BUYQUANTITY3)]
		public long? BuyQuantity3 { get; set; }

		/// <summary>
		/// giá mua 2
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYPRICE2)]
		[DataMember(Name = __SHORT_BUYPRICE2)]
		public double? BuyPrice2 { get; set; }

		/// <summary>
		/// KL mua 2
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYQUANTITY2)]
		[DataMember(Name = __SHORT_BUYQUANTITY2)]
		public long? BuyQuantity2 { get; set; }

		/// <summary>
		/// giá mua 1
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYPRICE1)]
		[DataMember(Name = __SHORT_BUYPRICE1)]
		public double? BuyPrice1 { get; set; }

		/// <summary>
		/// KL mua 1
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_BUYQUANTITY1)]
		[DataMember(Name = __SHORT_BUYQUANTITY1)]
		public long? BuyQuantity1 { get; set; }

		/// <summary>
		/// giá khớp
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_MATCHPRICE)]
		[DataMember(Name = __SHORT_MATCHPRICE)]
		public double? MatchPrice { get; set; }

		/// <summary>
		/// KL khớp
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_MATCHQUANTITY)]
		[DataMember(Name = __SHORT_MATCHQUANTITY)]
		public long? MatchQuantity { get; set; }

		/// <summary>
		/// thay đổi của mp so với giá tham chiếu
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_MATCHCHANGE)]
		[DataMember(Name = __SHORT_MATCHCHANGE)]
		public double? MatchChange { get; set; }

		/// <summary>
		/// thay đổi % của mp so với giá tham chiếu
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_MATCHCHANGEPERCENT)]
		[DataMember(Name = __SHORT_MATCHCHANGEPERCENT)]
		public double? MatchChangePercent { get; set; }

		/// <summary>
		/// Tổng KL đã khớp (khớp lệnh)
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_TOTALNMQUANTITY)]
		[DataMember(Name = __SHORT_TOTALNMQUANTITY)]
		public long? TotalNMQuantity { get; set; }

		/// <summary>
		/// giá bán 1
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLPRICE1)]
		[DataMember(Name = __SHORT_SELLPRICE1)]
		public double? SellPrice1 { get; set; }

		/// <summary>
		/// KL bán 1
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLQUANTITY1)]
		[DataMember(Name = __SHORT_SELLQUANTITY1)]
		public long? SellQuantity1 { get; set; }

		/// <summary>
		/// giá bán 2
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLPRICE2)]
		[DataMember(Name = __SHORT_SELLPRICE2)]
		public double? SellPrice2 { get; set; }

		/// <summary>
		/// KL bán 2
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLQUANTITY2)]
		[DataMember(Name = __SHORT_SELLQUANTITY2)]
		public long? SellQuantity2 { get; set; }

		/// <summary>
		/// giá bán 3
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLPRICE3)]
		[DataMember(Name = __SHORT_SELLPRICE3)]
		public double? SellPrice3 { get; set; }

		/// <summary>
		/// KL bán 3
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLQUANTITY3)]
		[DataMember(Name = __SHORT_SELLQUANTITY3)]
		public long? SellQuantity3 { get; set; }

		/// <summary>
		/// giá bán 4
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLPRICE4)]
		[DataMember(Name = __SHORT_SELLPRICE4)]
		public double? SellPrice4 { get; set; }

		/// <summary>
		/// KL bán 4
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLQUANTITY4)]
		[DataMember(Name = __SHORT_SELLQUANTITY4)]
		public long? SellQuantity4 { get; set; }

		/// <summary>
		/// giá bán 5
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLPRICE5)]
		[DataMember(Name = __SHORT_SELLPRICE5)]
		public double? SellPrice5 { get; set; }

		/// <summary>
		/// KL bán 5
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLQUANTITY5)]
		[DataMember(Name = __SHORT_SELLQUANTITY5)]
		public long? SellQuantity5 { get; set; }

		/// <summary>
		/// giá bán 6
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLPRICE6)]
		[DataMember(Name = __SHORT_SELLPRICE6)]
		public double? SellPrice6 { get; set; }

		/// <summary>
		/// KL bán 6
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLQUANTITY6)]
		[DataMember(Name = __SHORT_SELLQUANTITY6)]
		public long? SellQuantity6 { get; set; }

		/// <summary>
		/// giá bán 7
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLPRICE7)]
		[DataMember(Name = __SHORT_SELLPRICE7)]
		public double? SellPrice7 { get; set; }

		/// <summary>
		/// KL bán 7
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLQUANTITY7)]
		[DataMember(Name = __SHORT_SELLQUANTITY7)]
		public long? SellQuantity7 { get; set; }

		/// <summary>
		/// giá bán 8
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLPRICE8)]
		[DataMember(Name = __SHORT_SELLPRICE8)]
		public double? SellPrice8 { get; set; }

		/// <summary>
		/// KL bán 8
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLQUANTITY8)]
		[DataMember(Name = __SHORT_SELLQUANTITY8)]
		public long? SellQuantity8 { get; set; }

		/// <summary>
		/// giá bán 9
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLPRICE9)]
		[DataMember(Name = __SHORT_SELLPRICE9)]
		public double? SellPrice9 { get; set; }

		/// <summary>
		/// KL bán 9
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLQUANTITY9)]
		[DataMember(Name = __SHORT_SELLQUANTITY9)]
		public long? SellQuantity9 { get; set; }

		/// <summary>
		/// giá bán 10
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLPRICEX)]
		[DataMember(Name = __SHORT_SELLPRICEX)]
		public double? SellPriceX { get; set; }

		/// <summary>
		/// KL bán 10
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SELLQUANTITYX)]
		[DataMember(Name = __SHORT_SELLQUANTITYX)]
		public long? SellQuantityX { get; set; }

		/// <summary>
		/// số lệnh bán
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_OFFERCOUNT)]
		[DataMember(Name = __SHORT_OFFERCOUNT)]
		public long? OfferCount { get; set; }

		/// <summary>
		/// tổng KL bán
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_TOTALOFFERQTTY)]
		[DataMember(Name = __SHORT_TOTALOFFERQTTY)]
		public long? TotalOfferQtty { get; set; }

		/// <summary>
		/// giá mở cửa
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_OPENPRICE)]
		[DataMember(Name = __SHORT_OPENPRICE)]
		public double? OpenPrice { get; set; }

		/// <summary>
		/// giá trung bình
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_AVERAGEPRICE)]
		[DataMember(Name = __SHORT_AVERAGEPRICE)]
		public double? AveragePrice { get; set; }

		/// <summary>
		/// giá cao nhất
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_HIGHESTPRICE)]
		[DataMember(Name = __SHORT_HIGHESTPRICE)]
		public double? HighestPrice { get; set; }

		/// <summary>
		/// giá thấp nhất
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_LOWESTPRICE)]
		[DataMember(Name = __SHORT_LOWESTPRICE)]
		public double? LowestPrice { get; set; }

		/// <summary>
		/// KL nước ngoài mua
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_FOREIGNBUYQUANTITY)]
		[DataMember(Name = __SHORT_FOREIGNBUYQUANTITY)]
		public long? ForeignBuyQuantity { get; set; }

		/// <summary>
		/// KL nước ngoài bán
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_FOREIGNSELLQUANTITY)]
		[DataMember(Name = __SHORT_FOREIGNSELLQUANTITY)]
		public long? ForeignSellQuantity { get; set; }

		/// <summary>
		/// KL còn lại được phép mua
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_FOREIGNROOMREMAIN)]
		[DataMember(Name = __SHORT_FOREIGNROOMREMAIN)]
		public long? ForeignRoomRemain { get; set; }

		/// <summary>
		/// KL mở
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_OPENINTEREST)]
		[DataMember(Name = __SHORT_OPENINTEREST)]
		public double? OpenInterest { get; set; }

		/// <summary>
		/// ngày đáo hạn
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_LASTTRADINGDATE)]
		[DataMember(Name = __SHORT_LASTTRADINGDATE)]
		public string LastTradingDate { get; set; }

		/// <summary>
		/// Giao dịch không hưởng quyền
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_EXCLASSTYPE)]
		[DataMember(Name = __SHORT_EXCLASSTYPE)]
		public string ExClassType { get; set; }

		/// <summary>
		/// Ký hiệu để xác định loại giao dịch cho một mã chứng khoán.
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SYMBOLTRADINGMETHOD)]
		[DataMember(Name = __SHORT_SYMBOLTRADINGMETHOD)]
		public string SymbolTradingMethod { get; set; }

		/// <summary>
		/// "Ký hiệu để phân loại tình trạng giao dịch như tạm ngưng, ngưng giao dịch vì một số lý do."
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_SYMBOLTRADINGSANCTION)]
		[DataMember(Name = __SHORT_SYMBOLTRADINGSANCTION)]
		public string SymbolTradingSanction { get; set; }

		/// <summary>
		/// "Giá dự kiến được thực hiện trong phiên khớp lệnh định kỳ khi RE được áp dụng(Sự kiện RE xảy ra trong các đợt giao dịch khớp lệnh định kỳ.)"
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_TENTATIVEEXECUTIONPRICE)]
		[DataMember(Name = __SHORT_TENTATIVEEXECUTIONPRICE)]
		public double? TentativeExecutionPrice { get; set; }

		/// <summary>
		/// Giá dự kiến giao dịch (phiên định kỳ khớp lệnh mở cửa)
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_EXPECTEDTRADEPX)]
		[DataMember(Name = __SHORT_EXPECTEDTRADEPX)]
		public double? ExpectedTradePx { get; set; }

		/// <summary>
		/// Khối lượng dự kiến giao dịch (phiên định kỳ khớp lệnh mở cửa)
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_EXPECTEDTRADEQTY)]
		[DataMember(Name = __SHORT_EXPECTEDTRADEQTY)]
		public long? ExpectedTradeQty { get; set; }
	}
}
