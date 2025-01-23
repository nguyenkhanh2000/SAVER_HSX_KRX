using Newtonsoft.Json;
using System.Runtime.Serialization;

namespace MDDSCore.Boards
{
	/// <summary>
	/// 2020-10-01 15:48:20 ngocta2
	/// index data
	/// struct day du, mo ta chi tiet
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class BIndex : BBase
	{
		/// <summary>
		/// ID xác định các thị trường. VD: STO, BDO, DVX…
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_MARKETID)]
		[DataMember(Name = __SHORT_MARKETID)]
		public string MarketID { get; set; }

		/// <summary>
		/// ID phiên giao dịch. VD: 10, 40, 90, 99...
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_TRADINGSESSIONID)]
		[DataMember(Name = __SHORT_TRADINGSESSIONID)]
		public string TradingSessionID { get; set; }

		/// <summary>
		/// Thời gian thực thi HHmmSSsss
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_TRANSACTTIME)]
		[DataMember(Name = __SHORT_TRANSACTTIME)]
		public string TransactTime { get; set; }

		/// <summary>
		/// tên chỉ số index. VD: VNI, VN30, HNX30….
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_INDEX)]
		[DataMember(Name = __SHORT_INDEX)]
		public string Index { get; set; }

		/// <summary>
		/// giá trị của chỉ số index. VD: 1163.77
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_VALUE)]
		[DataMember(Name = __SHORT_VALUE)]
		public double? Value { get; set; }

		/// <summary>
		/// thay đổi so với index ngày GD trước
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_CHANGE)]
		[DataMember(Name = __SHORT_CHANGE)]
		public double? Change { get; set; }

		/// <summary>
		/// thay đổi % so với index ngày GD trước
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_CHANGEPERCENT)]
		[DataMember(Name = __SHORT_CHANGEPERCENT)]
		public double? ChangePercent { get; set; }

		/// <summary>
		/// Tổng khối lượng. tq=nmtq+pttq
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_TOTALQUANTITY)]
		[DataMember(Name = __SHORT_TOTALQUANTITY)]
		public long? TotalQuantity { get; set; }

		/// <summary>
		/// Tổng giá trị. tv=nmtv+pttv
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_TOTALVALUE)]
		[DataMember(Name = __SHORT_TOTALVALUE)]
		public double? TotalValue { get; set; }

		/// <summary>
		/// Tổng khối lượng GD theo  phương thức Khớp lệnh
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_NMTOTALQUANTITY)]
		[DataMember(Name = __SHORT_NMTOTALQUANTITY)]
		public long? NMTotalQuantity { get; set; }

		/// <summary>
		/// Tổng giá trị GD theo  phương thức Khớp lệnh
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_NMTOTALVALUE)]
		[DataMember(Name = __SHORT_NMTOTALVALUE)]
		public double? NMTotalValue { get; set; }

		/// <summary>
		/// Tổng khối lượng GD theo  phương thức Thỏa thuận
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_PTTOTALQUANTITY)]
		[DataMember(Name = __SHORT_PTTOTALQUANTITY)]
		public long? PTTotalQuantity { get; set; }

		/// <summary>
		/// Tổng giá trị GD theo  phương thức Thỏa thuận
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_PTTOTALVALUE)]
		[DataMember(Name = __SHORT_PTTOTALVALUE)]
		public double? PTTotalValue { get; set; }

		/// <summary>
		/// số mã có giá trần
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_CEILINGCOUNT)]
		[DataMember(Name = __SHORT_CEILINGCOUNT)]
		public int? CeilingCount { get; set; }

		/// <summary>
		/// số mã có giá tăng
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_UPCOUNT)]
		[DataMember(Name = __SHORT_UPCOUNT)]
		public int? UpCount { get; set; }

		/// <summary>
		/// số mã có giá không đổi
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_NOCHANGECOUNT)]
		[DataMember(Name = __SHORT_NOCHANGECOUNT)]
		public int? NochangeCount { get; set; }

		/// <summary>
		/// số mã có giá giảm
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_DOWNCOUNT)]
		[DataMember(Name = __SHORT_DOWNCOUNT)]
		public int? DownCount { get; set; }

		/// <summary>
		/// số mã có giá sàn
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_FLOORCOUNT)]
		[DataMember(Name = __SHORT_FLOORCOUNT)]
		public int? FloorCount { get; set; }


		/// <summary>
		/// index cua ngay giao dich truoc do (lay ra tu db)
		/// IndexChange = Index - LastIndex
		/// IgnoreDataMember - khi serialize thi bo qua property nay
		/// </summary>
		[IgnoreDataMember]
		public double? LastIndex { get; set; }

	}
}
