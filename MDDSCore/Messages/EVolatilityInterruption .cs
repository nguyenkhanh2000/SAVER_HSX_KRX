using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=5; name=Volatility Interruption; type=MD; Online</i></para>
	/// <para><b>Phân phối thông tin kiểm soát thị trường về việc xác định giá tham chiếu hoặc thay đổi giới hạn giá tự động</b></para>   
	/// <para>Thông tin này được tạo ra mỗi khi có sự kiện VI được kích hoạt. Chứng khoán sẽ chuyển từ phương thức giao dịch khớp lệnh liên tục sang phương thức khớp lệnh định kỳ.</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EVolatilityInterruption :EBase
    {
		/// <summary>
		/// MI = Volatility Interruption
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_VOLATILITY_INTERRUPTION;


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
		[JsonProperty(PropertyName  = __TAG_30001 , Order = 8)]
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
		[JsonProperty(PropertyName  = __TAG_20004 , Order = 9)]
		public string BoardID { get; set; }

		/// <summary>
		/// 2019-12-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName  = __TAG_55 , Order = 10)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2019-12-11 08:52:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20030  ; required=Y; format=String; length=1</i></para>
		/// <para><b>Phân loại mã kiểm soát biến động giá được áp dụng </b></para>
		/// <para>
		///1 = Kích hoạt kiểm soát biến động giá <br></br>
		///2 = Hủy kiểm soát biến động giá <br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName  = __TAG_20030 , Order = 11)]
		public string VITypeCode { get; set; }

		/// <summary>
		/// 2019-12-11 08:52:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20031  ; required=Y; format=String; length=1</i></para>
		/// <para><b>Phân loại mã kiểm soát biến động giá được áp dụng </b></para>
		/// <para>
		///1 = Kiểm soát biến động giá tĩnh <br></br>
		///2 = Kiểm soát biến động giá động<br></br>
		///3 = Kiểm soát biến động giá tĩnh + Kiểm soát biến động giá động<br></br> 
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName  = __TAG_20031 , Order = 12)]
		public string VIKindCode { get; set; }

		/// <summary>
		/// 2019-12-11 08:48:48 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20032    ; required=Y; format=Price; length=15 (9.4) </i></para>
		/// <para><b>Giá tham chiếu VI của kiểm soát biến động giá tĩnh </b></para>
		/// </summary>
		[JsonProperty(PropertyName  = __TAG_20032 , Order = 13)]
		public double StaticVIBasePrice { get; set; }  


		/// <summary>
		/// 2019-12-11 08:49:48 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20047 ; required=Y; format=Price; length=15 (9.4) </i></para>
		/// <para><b>Giá tham chiếu VI của kiểm soát biến động giá động.  </b></para>
		/// </summary>
		[JsonProperty(PropertyName  = __TAG_20047 , Order = 14)]
		public double DynamicVIBasePrice { get; set; }  


		/// <summary>
		/// 2019-12-11 08:49:58 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20033   ; required=Y; format=Price; length=15 (9.4) </i></para>
		/// <para><b>Giá kích hoạt của kiểm soát biến động giá </b></para>
		/// </summary>
		[JsonProperty(PropertyName  = __TAG_20033 , Order = 15)]
		public double VIPrice { get; set; }  


		/// <summary>
		/// 2019-12-11 08:49:58 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20034   ; required=Y; format=Price; length=8(3.3) </i></para>
		/// <para><b>Ngưỡng kích hoạt kiểm soát biến động giá tĩnh </b></para>
		/// </summary>
		[JsonProperty(PropertyName  = __TAG_20034 , Order = 16)]
		public double StaticVIDispartiyRatio { get; set; }  


		/// <summary>
		/// 2019-12-11 08:45:51 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20048   ; required=Y; format=Price; length=8(3.3) </i></para>
		/// <para><b>Ngưỡng kích hoạt kiểm soát biến động giá động  </b></para>
		/// </summary>
		[JsonProperty(PropertyName  = __TAG_20048 , Order = 17)]
		public double DynamicVIDispartiyRatio { get; set; }  


    }
}
