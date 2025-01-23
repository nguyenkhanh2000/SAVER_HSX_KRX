using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=9; name=Random End ; type=MW; Online</i></para>
	/// <para><b>Phân phối thông tin của kết thúc ngẫu nhiên khi điều kiện xảy ra bị kích hoạt</b></para>   
	/// <para>Cung cấp thông tin về sự kiện Random End khi sự kiện này được kích hoạt. Sự kiện RE xảy ra trong các đợt giao dịch khớp lệnh định kỳ.</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class ERandomEnd : EBase
    {
		/// <summary>
		/// MW = Random End 
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_RANDOM_END;

		


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
		/// 2019-12-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55, Order = 10)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2020-04-27 14:59:22 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=60 ; required=Y; format=UTCTime; length=9</i></para>
		/// <para><b>Thời gian thực thi HHmmSSsss</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_60, Order = 11)]
		public string TransactTime { get; set; }

		/// <summary>
		/// 2020-04-27 14:59:22 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30615; required=Y; format=String; length=1</i></para>
		/// <para><b>Mã được áp dụng cho kết thúc ngẫu nhiên</b></para>
		/// <para>
		/// 1: Thiết lập RE phiên khớp lệnh định kỳ mở cửa<br></br>
		/// 2: Hủy RE phiên khớp lệnh định kỳ mở cửa<br></br>
		/// 3: Thiết lập RE tại phiên khớp lệnh định kỳ đóng cửa<br></br>
		/// 4: Hủy RE phiên khớp lệnh định kỳ mở cửa<br></br>
		/// 5: Thiết lập RE cho thị trường ngoài giờ<br></br>
		/// 6: Hủy RE cho thị trường ngoài giờ<br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30615, Order = 12)]
		public string RandomEndApplyClassification { get; set; }

		/// <summary>
		/// 2020-04-27 14:59:22 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30616; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá dự kiến được thực hiện trong phiên khớp lệnh định kỳ khi RE được áp dụng</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30616 , Order = 13)]
		public double RandomEndTentativeExecutionPrice { get; set; }  

		/// <summary>
		/// 2020-04-27 14:59:22 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30617; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá dự kiến cao nhất được thực hiện trong phiên khớp lệnh định kỳ khi RE được áp dụng</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30617 , Order = 14)]
		public double RandomEndEstimatedHighestPrice { get; set; }  

		/// <summary>
		/// 2020-04-27 15:00:33 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30618; required=Y; format=Float; length=13(5.6)</i></para>
		/// <para><b>Tỷ lệ khác biệt giữa giá cao nhất dự kiến và giá dự kiến trong phiên khớp lệnh định kỳ</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30618 , Order = 15)]
		public double RandomEndEstimatedHighestPriceDisparateRatio { get; set; }   

		/// <summary>
		/// 2020-04-27 15:00:33 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30619; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá dự kiến thấp nhất được thực hiện trong phiên khớp lệnh định kỳ khi RE được áp dụng</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30619 , Order = 16)]
		public double RandomEndEstimatedLowestPrice { get; set; }  

		/// <summary>
		/// 2020-04-27 15:00:33 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30620; required=Y; format=Float; length=13(5.6)</i></para>
		/// <para><b>Tỷ lệ khác biệt giữa giá cao nhất dự kiến và giá dự kiến trong phiên khớp lệnh định kỳ</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30620 , Order = 17)]
		public double RandomEndEstimatedLowestPriceDisparateRatio { get; set; }  

		/// <summary>
		/// 2020-04-27 15:00:33 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30621; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá thực hiện gần nhất trước phiên khớp lệnh định kỳ</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30621 , Order = 18)]
		public double LatestPrice { get; set; }  

		/// <summary>
		/// 2020-04-27 15:00:57 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30622; required=Y; format=Float; length=13(5.6)</i></para>
		/// <para><b>Tỷ lệ khác biệt giữa giá khớp lệnh gần nhất và giá dự kiến trong phiên khớp lệnh định kỳ</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30622 , Order = 19)]
		public double LatestPriceDisparateRatio { get; set; }  

		/// <summary>
		/// 2020-04-27 15:00:57 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30623; required=Y; format=String; length=9</i></para>
		/// <para><b>Thời gian hủy RE HHmmSSsss</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30623, Order = 20)]
		public string RandomEndReleaseTimes { get; set; }


	}
}
