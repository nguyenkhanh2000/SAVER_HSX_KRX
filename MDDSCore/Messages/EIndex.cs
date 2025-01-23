using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=13; name=Index; type=M1; Batch/Online</i></para>  
	/// <para><b>Phân phối chỉ số theo định kỳ trong suốt thời gian giao dịch và khi thị trường đóng cửa(EOD) Các chỉ số được tính toán sau khi thị trường đóng cửa (EOD) được phân phối theo đợt</b></para>
	/// <para>Thông tin chỉ số cung cấp thông tin trực tuyến về chỉ số. Thông tin này được tạo ra định kỳ trong giờ giao dịch hoặc được tạo ra cuối ngày(từ thời điểm thị trường đóng cửa đến khi hoàn tất quy trình tính chỉ số).</para> 
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EIndex : EBase
    {

		/// <summary>
		/// M1 = Index
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_INDEX;

		/// <summary>
		/// 2019-11-28 14:58:10 hungtq
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
		[JsonProperty(PropertyName = __TAG_30001, Order = 8)]
		public string MarketID { get; set; }

		/// <summary>
		/// 2020-04-27 15:02:19 hungtq
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
		[JsonProperty(PropertyName = __TAG_336, Order = 9)]
		public string TradingSessionID { get; set; }

		/// <summary>
		/// 2020-04-27 15:02:19 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30569  ; required=Y; format=String; length=1 </i></para>
		/// <para><b>Mã thị trường của một chỉ số**Code**</b></para>
		/// <para>
		///1: HoSE<br></br>
		///2: HNX <br></br>
		///3: Chỉ số chung (HoSE + HNX) <br></br>
		///</para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30569, Order = 10)]
		public string MarketIndexClass { get; set; }

		/// <summary>
		/// 2020-04-27 15:02:19 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30167  ; required=Y; format=String; length=8 </i></para>
		/// <para><b>Mã của chỉ số được tính và công bố trên thị trường bao gồm chỉ số ngành, chỉ số đại diện thị trường, ví dụ VN Index, VN30, VNAllshares…</b></para>
		/// <para>
		///</para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30167, Order = 11)]
		public string IndexsTypeCode { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=15  ; required=Y; format=String; length=3 </i></para>
		/// <para><b>Mã tiền theo ISO 4217 VND, USD, EUR</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_15 , Order = 12)]
		public string Currency { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=60 ; required=Y; format=UTCTime; length=9</i></para>
		/// <para><b>Thời gian thực thi HHmmSSsss</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_60 , Order = 13)]
		public string TransactTime { get; set; }



		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30217 ; required=Y; format=Float; length=10(6.2)</i></para>
		/// <para><b>Giá trị chỉ số được tính</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30217 , Order = 14)]
		public double ValueIndexes { get; set; }   

		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=387 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng giao dịch lũy kế trong ngày</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_387 , Order = 15)]
		public long TotalVolumeTraded { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=381 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Tổng giá trị giao dịch lũy kế trong ngày</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_381 , Order = 16)]
		public double GrossTradeAmt { get; set; }   

		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30638 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng giao dịch theo phương thức khớp lệnh</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30638 , Order = 17)]
		public long ContauctAccTrdvol { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:02 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30639 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Tổng giá trị giao dịch theo phương thức khớp lệnh</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30639 , Order = 18)]
		public double ContauctAccTrdval { get; set; }   

		/// <summary>
		/// 2020-04-27 15:03:42 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30640 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng giao dịch theo phương thức thỏa thuận</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30640 , Order = 19)]
		public long BlktrdAccTrdvol { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:42 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30641 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Tổng giá trị giao dịch theo phương thức thỏa thuận</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30641 , Order = 20)]
		public double BlktrdAccTrdval { get; set; }   

		/// <summary>
		/// 2020-04-27 15:03:42 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30589 ; required=Y; format=Int; length=7</i></para>
		/// <para><b>Số chứng khoán có giá trần</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30589 , Order = 21)]
		public long FluctuationUpperLimitIssueCount { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30590 ; required=Y; format=Int; length=7</i></para>
		/// <para><b>Số chứng khoán có giá tăng</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30590 , Order = 22)]
		public long FluctuationUpIssueCount { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30591 ; required=Y; format=Int; length=7</i></para>
		/// <para><b>Số chứng khoán có giá không thay đổi</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30591 , Order = 23)]
		public long FluctuationSteadinessIssueCount { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30592 ; required=Y; format=Int; length=7</i></para>
		/// <para><b>Số chứng khoán có giá giảm</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30592 , Order = 24)]
		public long FluctuationDownIssueCount { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30593 ; required=Y; format=Int; length=7</i></para>
		/// <para><b>Số chứng khoán có giá sàn</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30593 , Order = 25)]
		public long FluctuationLowerLimitIssueCount { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30594 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng giao dịch của Chứng khoán màcó giá tăng hoặc tăng trần</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30594 , Order = 26)]
		public long FluctuationUpIssueVolume { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30595 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng giao dịch của Chứng khoán mà có giá giảm hoặc giảm sàn</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30595 , Order = 27)]
		public long FluctuationDownIssueVolume { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30596 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng giao dịch của Chứng khoán màcó giá không thay đổi</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30596 , Order = 28)]
		public long FluctuationSteadinessIssueVolume { get; set; }

		/// <summary>
		/// </summary>

		public string TransDate { get; set; }


		public EIndex() { }
		/// <summary>
		/// constructor
		/// </summary>
		/// <param name="rawData"></param>
		public EIndex(string rawData) : base(rawData)
		{
		}
	}
}
