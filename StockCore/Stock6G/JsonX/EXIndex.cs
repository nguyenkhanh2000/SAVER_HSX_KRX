using Newtonsoft.Json;
using System;
using System.Runtime.Serialization;
using SystemCore.Entities;

namespace StockCore.Stock6G.JsonX
{

    /// <summary>
    /// 2020-08-17 16:58:47 ngocta2
    /// struct index
    /// giong 5G 
    /// https://liveprice.fpts.com.vn/hsx/data.ashx?s=index
    /// https://liveprice.fpts.com.vn/hnx/data.ashx?s=index
    /// </summary>
    [JsonObject(MemberSerialization.OptIn)]
    public class EXIndex
    {
		/// <summary>
		/// 2019-11-28 14:58:10 hungtq
		/// MarketID	ID xác định các thị trường. VD: STO, BDO, DVX…
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30001; required=Y; format=String; length=3</i></para>
		/// <para><b>ID xác định các thị trường</b></para>
		/// <para>
		///  Sở giao dịch chứng khoán Tp Hồ Chí Minh <br></br>
		/// - STO: Thị trường cổ phiếu<br></br> 
		/// - BDO: Thị trường trái phiếu<br></br> 
		/// - RPO: Thị trường Repo
		/// </para>
		/// <para>
		///  Sở giao dịch chứng khoán Hà Nội<br></br>
		/// - STX: Thị trường cổ phiếu<br></br> 
		/// - BDX: Thị trường trái phiếu chính phủ<br></br> 
		/// - DVX: Thị trường phái sinh
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = "m", Order = 1)]
		public string MarketID { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
		public long MarketIDSeqNum { get; set; }

		/// <summary>
		/// 2020-04-27 15:02:19 hungtq
		/// TradingSessionID	ID phiên giao dịch. VD: 10, 40, 90, 99...
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=336 ; required=Y; format=String; length=2</i></para>
		/// <para>
		///01 = Nạp lại Lệnh GT <br></br>
		///10 = Phiên mở cửa <br></br>
		///11 = Phiên mở cửa (mở rộng) <br></br>
		///20 = Phiên định kỳ sau khi dừng thị trường <br></br>
		///21 = Phiên định kỳ sau khi dừng thị trường (mở rộng) <br></br>
		///30 = Kết thúc phiên định kỳ <br></br>
		///40 = Phiên liên tục <br></br>
		///50 = Kiểm soát biến động giá <br></br>
		///51 = Kiểm soát biến động giá (mở rộng) <br></br>
		///60 = Tiếp nhận giá đóng cửa sau khi đóng cửa <br></br>
		///80 = Phiên khớp lệnh định kỳ nhiều lần <br></br>
		///90 = Tạm ngừng giao dịch <br></br>
		///99 = Đóng cửa thị trường <br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = "ts", Order = 2)]
		public string TradingSessionID { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
		public long TradingSessionIDSeqNum { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// TransactTime	Thời gian thực thi HHmmSSsss
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=60 ; required=Y; format=UTCTime; length=9</i></para>
		/// <para><b>Thời gian thực thi HHmmSSsss</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "tt", Order = 3)]
		public string TransactTime { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
		public long TransactTimeSeqNum { get; set; }

		/// <summary>
		/// 2020-04-27 15:02:19 hungtq
		/// Index/IndexsTypeCode	tên chỉ số index. VD: VNI, VN30, HNX30….
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30167  ; required=Y; format=String; length=8 </i></para>
		/// <para><b>Mã của chỉ số được tính và công bố trên thị trường bao gồm chỉ số ngành, chỉ số đại diện thị trường, ví dụ VN Index, VN30, VNAllshares…</b></para>
		/// <para>
		///</para>
		/// </summary>
		[JsonProperty(PropertyName = "i", Order = 4)]
		public string Index { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
		public long IndexSeqNum { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// Value	giá trị của chỉ số index. VD: 1163.77
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30217 ; required=Y; format=Float; length=10(6.2)</i></para>
		/// <para><b>Giá trị chỉ số được tính</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "v", Order = 5)]
		public double Value { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
		public long ValueSeqNum { get; set; }

		/// <summary>
		/// 2020-08-17 17:02:52 ngocta2
		/// Change	thay đổi so với index ngày GD trước
		/// thay doi so voi last index => tu tinh
		/// </summary>
		[JsonProperty(PropertyName = "c", Order = 6)]
		public double Change { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
		public long ChangeSeqNum { get; set; }

		/// <summary>
		/// 2020-08-17 17:02:52 ngocta2
		/// ChangePercent	thay đổi % so với index ngày GD trước
		/// thay doi % so voi last index => tu tinh
		/// </summary>
		[JsonProperty(PropertyName = "cp", Order = 7)]
		public double ChangePercent { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
		public long ChangePercentSeqNum { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// TotalQuantity/TotalVolumeTraded	Tổng khối lượng. tq=nmq+ptq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=387 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng giao dịch lũy kế trong ngày</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "tq", Order = 8)]
		public long TotalQuantity { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
		public long TotalQuantitySeqNum { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// TotalValue/GrossTradeAmt	Tổng giá trị. tv=nmv+ptv
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=381 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Tổng giá trị giao dịch lũy kế trong ngày</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "tv", Order = 9)]
		public double TotalValue { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
		public long TotalValueSeqNum { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// TotalQuantity/ContauctAccTrdvol	Tổng khối lượng GD theo  phương thức Khớp lệnh
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30638 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng giao dịch theo phương thức khớp lệnh</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "nmtq", Order = 10)]
		public long NMTotalQuantity { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
		public long NMTotalQuantitySeqNum { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// TotalValue/ContauctAccTrdval	Tổng giá trị GD theo  phương thức Khớp lệnh
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30639 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Tổng giá trị giao dịch theo phương thức khớp lệnh</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "nmtv", Order = 11)]
		public double NMTotalValue { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
		public long NMTotalValueSeqNum { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:42 hungtq
		/// TotalQuantity/BlktrdAccTrdvol	Tổng khối lượng GD theo  phương thức Thỏa thuận
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30640 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng giao dịch theo phương thức thỏa thuận</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "pttq", Order = 12)]
		public long PTTotalQuantity { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
		public long PTTotalQuantitySeqNum { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:42 hungtq
		/// TotalValue/BlktrdAccTrdval	Tổng giá trị GD theo  phương thức Thỏa thuận
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30641 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Tổng giá trị giao dịch theo phương thức thỏa thuận</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "pttv", Order = 13)]
		public double PTTotalValue { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
		public long PTTotalValueSeqNum { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:42 hungtq
		/// CeilingCount/FluctuationUpperLimitIssueCount	số mã có giá trần
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30589 ; required=Y; format=Int; length=7</i></para>
		/// <para><b>Số chứng khoán có giá trần</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "cc", Order = 14)]
		public long CeilingCount { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
		public long CeilingCountSeqNum { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// UpCount/FluctuationUpIssueCount	số mã có giá tăng
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30590 ; required=Y; format=Int; length=7</i></para>
		/// <para><b>Số chứng khoán có giá tăng</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "uc", Order = 15)]
		public long UpCount { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
		public long UpCountSeqNum { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// NochangeCount/FluctuationSteadinessIssueCount	số mã có giá không đổi
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30591 ; required=Y; format=Int; length=7</i></para>
		/// <para><b>Số chứng khoán có giá không thay đổi</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "nc", Order = 16)]
		public long NochangeCount { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
		public long NochangeCountSeqNum { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// DownCount/FluctuationDownIssueCount	số mã có giá giảm
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30592 ; required=Y; format=Int; length=7</i></para>
		/// <para><b>Số chứng khoán có giá giảm</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "dc", Order = 17)]
		public long DownCount { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
		public long DownCountSeqNum { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// FloorCount/FluctuationLowerLimitIssueCount	số mã có giá sàn
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30593 ; required=Y; format=Int; length=7</i></para>
		/// <para><b>Số chứng khoán có giá sàn</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "fc", Order = 18)]
		public long FloorCount { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
		public long FloorCountSeqNum { get; set; }


		/// <summary>
		/// 2021-03-31 10:26:13 ngocta2
		/// index value của phiên giao dịch liền trước, data này phải lấy từ db hist, 
		/// nhưng có thể cache vào redis để khỏi tạo connection vào db SQL (mssql/oracle)
		/// </summary>
		public double LastValue { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
		public long LastValueSeqNum { get; set; }

		/// <summary>
		/// 2021-10-22 14:04:17 ngocta2
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=75 ; required=Y; format=LocalMkt Date; length=8</i></para>
		/// <para><b>Ngày giao dịch. Định dạng (YYYYMMDD)</b></para>
		/// </summary>
		[DataMember(Name = "td")]
		[JsonProperty(PropertyName = "td", Order = 19)]
		public string TradeDate { get; set; }
		public long TradeDateSeqNum { get; set; }		

		//public EXIndex(double lastValue) => LastValue = lastValue;

		/// <summary>
		/// constructor 1
		/// </summary>
		public EXIndex() { }

		/// <summary>
		/// constructor 2
		/// </summary>
		/// <param name="seqNum"></param>
		public EXIndex(long seqNum)
        {
			this.MarketIDSeqNum         = seqNum;
			this.TradingSessionIDSeqNum = seqNum;
			this.TransactTimeSeqNum     = seqNum;
			this.IndexSeqNum            = seqNum;
			this.ValueSeqNum            = seqNum;
			this.ChangeSeqNum           = seqNum;
			this.ChangePercentSeqNum    = seqNum;
			this.TotalQuantitySeqNum    = seqNum;
			this.TotalValueSeqNum       = seqNum;
			this.NMTotalQuantitySeqNum  = seqNum;
			this.NMTotalValueSeqNum     = seqNum;
			this.PTTotalQuantitySeqNum  = seqNum;
			this.PTTotalValueSeqNum     = seqNum;
			this.CeilingCountSeqNum     = seqNum;
			this.UpCountSeqNum          = seqNum;
			this.NochangeCountSeqNum    = seqNum;
			this.DownCountSeqNum        = seqNum;
			this.FloorCountSeqNum       = seqNum;
			this.LastValueSeqNum        = seqNum;
			this.TradeDateSeqNum        = seqNum;
		}


		public void CalculateChange()
		{
			Change = Value - LastValue;
			if (LastValue == 0)
				ChangePercent = 0;
			else
				ChangePercent = (Change / LastValue) * 100;
		}
	}

	 /// <summary>
    /// tuong tu EXIndex nhung class nay de rieng cho serialize
    /// https://github.com/neuecc/MessagePack-CSharp/issues/859
    /// There is no option to ignore the null value? Like Json.Net?
    /// Correct: MessagePack does not offer that option. => tam thoi bo qua MessagePack  => dung Utf8Json
    /// -------------
    /// using MessagePack;
    /// ---------------
    /// nuget install MessagePack
    /// nuget install MessagePackAnalyzer
    /// </summary>
    
    public class EXIndexS : EXBase
    {
        [DataMember(Name = "m")]
		[JsonProperty(PropertyName = "m", Order = 1)]
		public string? MarketID { get; set; }

		[DataMember(Name = "ts")]
		[JsonProperty(PropertyName = "ts", Order = 2)]
		public string? TradingSessionID { get; set; }

		[DataMember(Name = "tt")]
		[JsonProperty(PropertyName = "tt", Order = 3)]
		public string? TransactTime { get; set; }

		[DataMember(Name = "i")]
		[JsonProperty(PropertyName = "i", Order = 4)]
		public string? Index { get; set; }

		[DataMember(Name = "v")]
		[JsonProperty(PropertyName = "v", Order = 5)]
		public double? Value { get; set; }

		[DataMember(Name = "c")]
		[JsonProperty(PropertyName = "c", Order = 6)]
		public double? Change { get; set; }

		[DataMember(Name = "cp")]
		[JsonProperty(PropertyName = "cp", Order = 7)]
		public double? ChangePercent { get; set; }

		[DataMember(Name = "tq")]
		[JsonProperty(PropertyName = "tq", Order = 8)]
		public long? TotalQuantity { get; set; }

		[DataMember(Name = "tv")]
		[JsonProperty(PropertyName = "tv", Order = 9)]
		public double? TotalValue { get; set; }

		[DataMember(Name = "nmtq")]
		[JsonProperty(PropertyName = "nmtq", Order = 10)]
		public long? NMTotalQuantity { get; set; }

		[DataMember(Name = "nmtv")]
		[JsonProperty(PropertyName = "nmtv", Order = 11)]
		public double? NMTotalValue { get; set; }

		[DataMember(Name = "pttq")]
		[JsonProperty(PropertyName = "pttq", Order = 12)]
		public long? PTTotalQuantity { get; set; }

		[DataMember(Name = "pttv")]
		[JsonProperty(PropertyName = "pttv", Order = 13)]
		public double? PTTotalValue { get; set; }

		[DataMember(Name = "cc")]
		[JsonProperty(PropertyName = "cc", Order = 14)]
		public long? CeilingCount { get; set; }

		[DataMember(Name = "uc")]
		[JsonProperty(PropertyName = "uc", Order = 15)]
		public long? UpCount { get; set; }

		[DataMember(Name = "nc")]
		[JsonProperty(PropertyName = "nc", Order = 16)]
		public long? NochangeCount { get; set; }

		[DataMember(Name = "dc")]
		[JsonProperty(PropertyName = "dc", Order = 17)]
		public long? DownCount { get; set; }

		[DataMember(Name = "fc")]
		[JsonProperty(PropertyName = "fc", Order = 18)]
		public long? FloorCount { get; set; }

		[DataMember(Name = "lv")]
		[JsonProperty(PropertyName = "lv", Order = 18)]
		public double? LastValue { get; set; }

		[DataMember(Name = "td")]
		[JsonProperty(PropertyName = "td", Order = 19)]
		public string? TradeDate { get; set; }
#nullable disable

		/// <summary>
		/// cac truong hop data raw voi field ko co data thi khi serialize se ko output ra field do
		/// chu y truong hop data tai cot top 10 buy sell, gia tri null khac gia tri 0
		/// + gia tri 0    la xoa data tai do do
		/// + gia tri null la bo qua data tai o do ( co the co dang co hoac ko co data )
		/// </summary>
		/// <param name="xIndex"></param>
		public EXIndexS(EXIndex xIndex)
        {
			if (xIndex.MarketID         == EGlobalConfig.__INIT_NULL_STRING) this.MarketID         = null; else this.MarketID         = xIndex.MarketID;
			if (xIndex.TradingSessionID == EGlobalConfig.__INIT_NULL_STRING) this.TradingSessionID = null; else this.TradingSessionID = xIndex.TradingSessionID;
			if (xIndex.TransactTime     == EGlobalConfig.__INIT_NULL_STRING) this.TransactTime     = null; else this.TransactTime     = xIndex.TransactTime;
			if (xIndex.Index            == EGlobalConfig.__INIT_NULL_STRING) this.Index            = null; else this.Index            = xIndex.Index;
			if (xIndex.Value            == EGlobalConfig.__INIT_NULL_DOUBLE) this.Value            = null; else this.Value            = xIndex.Value;
			if (xIndex.Change           == EGlobalConfig.__INIT_NULL_DOUBLE) this.Change           = null; else this.Change           = xIndex.Change;
			if (xIndex.ChangePercent    == EGlobalConfig.__INIT_NULL_DOUBLE) this.ChangePercent    = null; else this.ChangePercent    = xIndex.ChangePercent;
			if (xIndex.TotalQuantity    == EGlobalConfig.__INIT_NULL_LONG)   this.TotalQuantity    = null; else this.TotalQuantity    = xIndex.TotalQuantity;
			if (xIndex.TotalValue       == EGlobalConfig.__INIT_NULL_DOUBLE) this.TotalValue       = null; else this.TotalValue       = xIndex.TotalValue;
			if (xIndex.NMTotalQuantity  == EGlobalConfig.__INIT_NULL_LONG)   this.NMTotalQuantity  = null; else this.NMTotalQuantity  = xIndex.NMTotalQuantity;
			if (xIndex.NMTotalValue     == EGlobalConfig.__INIT_NULL_DOUBLE) this.NMTotalValue     = null; else this.NMTotalValue     = xIndex.NMTotalValue;
			if (xIndex.PTTotalQuantity  == EGlobalConfig.__INIT_NULL_LONG)   this.PTTotalQuantity  = null; else this.PTTotalQuantity  = xIndex.PTTotalQuantity;
			if (xIndex.PTTotalValue     == EGlobalConfig.__INIT_NULL_DOUBLE) this.PTTotalValue     = null; else this.PTTotalValue     = xIndex.PTTotalValue;
			if (xIndex.CeilingCount     == EGlobalConfig.__INIT_NULL_LONG)   this.CeilingCount     = null; else this.CeilingCount     = xIndex.CeilingCount;
			if (xIndex.UpCount          == EGlobalConfig.__INIT_NULL_LONG)   this.UpCount          = null; else this.UpCount          = xIndex.UpCount;
			if (xIndex.NochangeCount    == EGlobalConfig.__INIT_NULL_LONG)   this.NochangeCount    = null; else this.NochangeCount    = xIndex.NochangeCount;
			if (xIndex.DownCount        == EGlobalConfig.__INIT_NULL_LONG)   this.DownCount        = null; else this.DownCount        = xIndex.DownCount;
			if (xIndex.FloorCount       == EGlobalConfig.__INIT_NULL_LONG)   this.FloorCount       = null; else this.FloorCount       = xIndex.FloorCount;
			if (xIndex.LastValue		== EGlobalConfig.__INIT_NULL_DOUBLE) this.LastValue		   = null; else this.LastValue		  = xIndex.LastValue;
			if (xIndex.TradeDate		== EGlobalConfig.__INIT_NULL_STRING) this.TradeDate		   = null; else this.TradeDate		  = xIndex.TradeDate;

			// co gia last index (hom gd truoc) va value index now thi tinh ra thay doi (C) va thay doi phan tram (CP)
			if (xIndex.LastValue != EGlobalConfig.__INIT_NULL_DOUBLE && xIndex.Value != EGlobalConfig.__INIT_NULL_DOUBLE && xIndex.LastValue != 0)
            {
                this.Change        = xIndex.Value - xIndex.LastValue; // 35.23454747
                this.ChangePercent = (this.Change / xIndex.LastValue) * 100; // -6.756756756756757
                this.ChangePercent = System.Math.Round(Convert.ToDouble( this.ChangePercent) , 2); // -6.76
				this.Change		   = System.Math.Round(Convert.ToDouble(this.Change), 2); // 35.23
			}
        }
        
    }
}
