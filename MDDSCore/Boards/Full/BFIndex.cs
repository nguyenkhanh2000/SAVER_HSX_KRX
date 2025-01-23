using MDDSCore.Boards.Short;
using Newtonsoft.Json;

namespace MDDSCore.Boards.Full
{
	/// <summary>
	/// 2020-10-01 15:48:20 ngocta2
	/// index data
	/// struct day du, mo ta chi tiet
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class BFIndex : BBase
	{
		/// <summary>
		/// ID xác định các thị trường. VD: STO, BDO, DVX…
		/// </summary>
		public string MarketID { get; set; }

		/// <summary>
		/// ID phiên giao dịch. VD: 10, 40, 90, 99...
		/// </summary>
		public string TradingSessionID { get; set; }

		/// <summary>
		/// Thời gian thực thi HHmmSSsss
		/// </summary>
		public string TransactTime { get; set; }

		/// <summary>
		/// tên chỉ số index. VD: VNI, VN30, HNX30….
		/// </summary>
		public string Index { get; set; }

		/// <summary>
		/// giá trị của chỉ số index. VD: 1163.77
		/// </summary>
		public double Value { get; set; }

		/// <summary>
		/// thay đổi so với index ngày GD trước
		/// </summary>
		public double Change { get; set; }

		/// <summary>
		/// thay đổi % so với index ngày GD trước
		/// </summary>
		public double ChangePercent { get; set; }

		/// <summary>
		/// Tổng khối lượng. tq=nmtq+pttq
		/// </summary>
		public long TotalQuantity { get; set; }

		/// <summary>
		/// Tổng giá trị. tv=nmtv+pttv
		/// </summary>
		public double TotalValue { get; set; }

		/// <summary>
		/// Tổng khối lượng GD theo  phương thức Khớp lệnh
		/// </summary>
		public long NMTotalQuantity { get; set; }

		/// <summary>
		/// Tổng giá trị GD theo  phương thức Khớp lệnh
		/// </summary>
		public double NMTotalValue { get; set; }

		/// <summary>
		/// Tổng khối lượng GD theo  phương thức Thỏa thuận
		/// </summary>
		public long PTTotalQuantity { get; set; }

		/// <summary>
		/// Tổng giá trị GD theo  phương thức Thỏa thuận
		/// </summary>
		public double PTTotalValue { get; set; }

		/// <summary>
		/// số mã có giá trần
		/// </summary>
		public int CeilingCount { get; set; }

		/// <summary>
		/// số mã có giá tăng
		/// </summary>
		public int UpCount { get; set; }

		/// <summary>
		/// số mã có giá không đổi
		/// </summary>
		public int NochangeCount { get; set; }

		/// <summary>
		/// số mã có giá giảm
		/// </summary>
		public int DownCount { get; set; }

		/// <summary>
		/// số mã có giá sàn
		/// </summary>
		public int? FloorCount { get; set; }

		// ===============================================================

		public BFIndex() { }
		public BFIndex(BSIndex entity)
		{
			this.MarketID         = entity.m;
			this.TradingSessionID = entity.ts;
			this.TransactTime     = entity.tt;
			this.Index            = entity.i;
			this.Value            = entity.v;
			this.Change           = entity.c;
			this.ChangePercent    = entity.cp;
			this.TotalQuantity    = entity.tq;
			this.TotalValue       = entity.tv;
			this.NMTotalQuantity  = entity.nmtq;
			this.NMTotalValue     = entity.nmtv;
			this.PTTotalQuantity  = entity.pttq;
			this.PTTotalValue     = entity.pttv;
			this.CeilingCount     = entity.cc;
			this.UpCount          = entity.uc;
			this.NochangeCount    = entity.nc;
			this.DownCount        = entity.dc;
			this.FloorCount       = entity.fc;
		}
	}
}
