using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=30; name=Trading Result of Foreign Investors; type=MT; Online</i></para>
	/// <para><b>Phân phối thông tin thống kê giao dịch của nhà đầu tư nước ngoài cho mỗi cổ phiếu</b></para>   
	/// <para>Thông tin này cung cấp tổng giao dịch của nhà đầu tư nước ngoài đối với từng mã chứng khoán.</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class ETradingResultOfForeignInvestors : EBase
    {
		/// <summary>
		/// MT = Trading Result of Foreign Investors
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_TRADING_RESULT_OF_FOREIGN_INVESTORS;


		/// <summary>
		/// 2019-12-10 11:58:10 hungtq
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
		/// - UPX: Thị trường UPCOM 
		/// - HCX: Thị trường trái phiếu doanh nghiệp 
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30001, Order = 8)]
		public string MarketID { get; set; }

		/// <summary>
		/// 2019-12-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20004; required=Y; format=String; length=2</i></para>
		/// <para><b>ID Bảng giao dịch</b></para>
		/// <para>
		/// G1 : Chính(Main)<br></br>
		/// G2 : Trước giờ giao dịch(Pre Open)<br></br>
		/// G3 : Sau giờ giao dịch(Post Close)<br></br>
		/// G4 : Lô lẻ(Odd lot)<br></br>
		/// G7 : Mua bắt buộc(Buy-in)<br></br>
		/// G8 : Bán bắt buộc(Sell-out)<br></br>
		/// T1 : Thỏa thuận(regular)<br></br>
		/// T4 : Thỏa thuận lô lẻ(regular for Odd lot)<br></br>
		/// T2 : Thỏa thuận trước giờ giao dịch(pre)<br></br>
		/// T3 : Thỏa thuận sau giờ giao dịch(post)<br></br>
		/// T5 : Thỏa thuận sau giờ giao dịch cho lô lẻ(post for Odd lot)<br></br>
		/// R1 : Thỏa thuận(Repo)<br></br>
		/// AL : Tất cả Bảng giao dịch<br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20004, Order = 9)]
		public string BoardID { get; set; }

		/// <summary>
		/// 2020-04-27 15:12:40 hungtq
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
		[JsonProperty(PropertyName = __TAG_336, Order = 10)]
		public string TradingSessionID { get; set; }

		/// <summary>
		/// 2020-04-27 15:12:40 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55, Order = 11)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2020-04-27 14:59:22 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=60 ; required=Y; format=UTCTime; length=9</i></para>
		/// <para><b>Thời gian thực thi HHmmSSsss</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_60, Order = 12)]
		public string TransactTime { get; set; }

		/// <summary>
		/// 2020-04-27 15:12:40 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20054 ; required=Y; format=String; length=2</i></para>
		/// <para><b>Mã phân loại nhà đầu</b></para>
		/// <para>
		/// 00 = Trong nước<br></br>
		/// 10 = Nước ngoài<br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20054, Order = 13)]
		public string FornInvestTypeCode { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=331 ; required=Y; format=Int; length=10</i></para>
		/// <para><b>Khối lượng giao dịch bên bán</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_331, Order = 14)]
		public long SellVolume { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30168 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Giá trị giao dịch bên bán</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30168 , Order = 15)]
		public double SellTradeAmount { get; set; }   

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=330 ; required=Y; format=Int; length=10</i></para>
		/// <para><b>Khối lượng giao dịch bên mua</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_330, Order = 16)]
		public long BuyVolume { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30169 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Giá trị giao dịch bên mua</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30169 , Order = 17)]
		public double BuyTradedAmount { get; set; }   

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30643 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng giao dịch bên bán</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30643, Order = 18)]
		public long SellVolumeTotal { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30644 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Tổng giá trị giao dịch bên bán</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30644 , Order = 19)]
		public double SellTradeAmountTotal { get; set; }   

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30645 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng Khối lượng giao dịch bên mua</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30645, Order = 20)]
		public long BuyVolumeTotal { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30646 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Tổng Giá trị giao dịch bên mua</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30646 , Order = 21)]
		public double BuyTradeAmountTotal { get; set; }   

		
	}
}
