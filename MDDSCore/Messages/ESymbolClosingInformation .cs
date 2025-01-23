using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=4; name= Symbol Closing Information; type=M8; Batch</i></para>
	/// <para><b>Phân phối thông tin kiểm soát thị trường về việc đóng cửa mã cổ phiếu</b></para>   
	/// <para>Thông tin này được tạo ra vào cuối ngày cung cấp giá khớp cuối cùng của ngày đó. Trường hợp không có giá khớp, thì giá đóng cửa sẽ là giá tham chiếu, trường hợp không có giá tham chiếu thì giá đóng cửa sẽ bằng không.</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class ESymbolClosingInformation : EBase
    {
		/// <summary>
		/// M8 =  Symbol Closing Information
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_SYMBOL_CLOSING_INFORMATION;

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
		[JsonProperty(PropertyName = __TAG_30001 , Order = 8)]
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
		[JsonProperty(PropertyName = __TAG_20004 , Order = 9)]
		public string BoardID { get; set; }

		/// <summary>
		/// 2019-12-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55 , Order = 10)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2019-12-11 8:48:10 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20026  ; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá thực hiện cuối cùng của ngày đó. Nếu không có thực hiện, nó sẽ được thay thế bằng giá tham chiếu và nếu không có giá tham chiếu thì nó sẽ là 0 (không)  </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20026 , Order = 11)]
		public double SymbolCloseInfoPx { get; set; }  


		/// <summary>
		/// 2019-12-11 08:48:48 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30541   ; required=Y; format=Price; length=13(5.6) </i></para>
		/// <para><b>Tỷ lệ lợi tức của trái phiếu cho thông tin đóng cửa </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30541 , Order = 12)]
		public double SymbolCloseInfoYield { get; set; }

		/// <summary>
		/// 2019-12-11 08:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20027 ; required=Y; format=String; length=1</i></para>
		/// <para><b>Phương thức của giá đóng cửa:</b></para>
		/// <para>
		///1: Giá thực hiện cuối cùng <br></br>
		///2: Giá đặt lệnh tốt nhất  <br></br>
		///3: Không có giao dịch <br></br>
		///4: "Giá đặt lệnh tốt nhất đối với chứng khoán giao dịch theo phương thức Giá khớp lệnh đầu tiên" <br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20027 , Order = 13)]
		public string SymbolCloseInfoPxType { get; set; }

	}
}
