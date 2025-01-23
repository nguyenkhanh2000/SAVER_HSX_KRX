using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2019-11-28 15:59:24 ngocta2
	/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
	/// <para><i>no=2; name=Security Status; type=f; Online</i></para>
	/// <para><b>Phân phối thông tin kiểm soát thị trường để thay đổi trạng thái của một cổ phiếu</b></para>
	/// <para><b>Thông tin này cung cấp thông tin về về tình trạng giao dịch của thị trường. Thông tin này được gửi mỗi khi có sự thay đổi (bắt đầu/kết thúc) bảng giao dịch hay đợt giao dịch.</b></para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class ESecurityStatus : EBase
	{

		/// <summary>
		/// f = Security Status
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_SECURITY_STATUS;

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
		[JsonProperty(PropertyName = __TAG_30001 , Order = 8)]
		public string MarketID { get; set; }

		/// <summary>
		/// 2019-11-28 14:58:10 hungtq
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
		/// 2019-11-28 16:04:10 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20005; required=Y; format=String; length=3</i></para>
		/// <para><b>Xác định hoạt động bảng giao dịch. Về cơ bản, lịch trình giao dịch là một loạt các hoạt động sự kiện được xác định bẳng một mã 3 ký tự, theo đây là ví dụ:</b></para>
		/// <para>
		/// 0915: Opening call auction (AA1) <br></br>
		/// 0930: Continuous trading (BB1) <br></br>
		/// 1430: Closing call auction (BC1) <br></br>
		/// 1445: Market close (AC2) <br></br>
		/// AA1 : Opening CA Open <br></br>
		/// AB1 : Cont.A Open <br></br>
		/// AB2 : Board Trading Close <br></br>
		/// AC2 : Closing CA Execution <br></br>
		/// AD2 : CA Excecution <br></br>
		/// AE1 : Market Halt Release <br></br>
		/// AE8 : Market Halt <br></br>
		/// AF1 : CA Open after CB <br></br>
		/// AF8 : Suspension by CB <br></br>
		/// AW8 : Lunch Break <br></br>
		/// AW9 : Open after Lunch Break <br></br>
		/// BB1 : Cont.A Open for Non-PCA Instr. <br></br>
		/// BC1 : Closing CA Open <br></br>
		/// BE9 : Cont.A Open after Market Halt <br></br>
		/// BF9 : Cont.A Open after CB <br></br>
		/// CC1 : Closing PCA Open <br></br>
		/// CD1 : PCA Open <br></br>
		/// DB2 : Trading Close for LTD Instr. <br></br>
		/// DC2 : Closing CA Execution for LTD Instr. <br></br>
		/// EI1 : Trading Halt Release by Instr./Product <br></br>
		/// EI8 : Instr./Product Halt <br></br>
		/// EI9 : Cont.A Open after Instr./Product Halt
		/// </para>
		/// </summary>
	   [JsonProperty(PropertyName = __TAG_20005 , Order = 10)]
		public string BoardEvtID { get; set; }

		/// <summary>
		/// 2019-12-10 14:30:10 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20008 ; required=N; format=String; length=2</i></para>
		/// <para><b>Mã bắt đầu/kết thúc đợt giao dịch/ Bảng giao dịch </b></para>
		/// <para>
		///BS : Bắt đầu Bảng giao dịch<br></br>		///BE : Kết thúc Bảng giao dịch<br></br>		///SS : Bắt đầu Đợt giao dịch <br></br>		///SE : Kết thúc Đợt giao dịch <br></br>		///SH : Tạm dừng Đợt giao dịch <br></br>		///SR : Bắt đầu lại Đợt giao dịch<br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20008 , Order = 11)]
		public string SessOpenCloseCode { get; set; }

		/// <summary>
		/// 2019-12-10 14:30:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55 , Order = 12)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2019-12-10 14:31:10 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=336 ; required=Y; format=String; length=2</i></para>
		/// <para>
		///01 = Nạp lại Lệnh GT <br></br>		///10 = Phiên mở cửa <br></br>		///11 = Phiên mở cửa (mở rộng) <br></br>		///20 = Phiên định kỳ sau khi dừng thị trường <br></br>		///21 = Phiên định kỳ sau khi dừng thị trường (mở rộng) <br></br>		///30 = Kết thúc phiên định kỳ <br></br>		///40 = Phiên liên tục <br></br>		///50 = Kiểm soát biến động giá <br></br>		///51 = Kiểm soát biến động giá (mở rộng) <br></br>		///60 = Tiếp nhận giá đóng cửa sau khi đóng cửa <br></br>		///80 = Phiên khớp lệnh định kỳ nhiều lần <br></br>		///90 = Tạm ngừng giao dịch <br></br>		///99 = Đóng cửa thị trường <br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_336 , Order = 13)]
		public string TradingSessionID { get; set; }

        [JsonProperty(PropertyName = __TAG_30648, Order = 14)]
        public string TscProdGrpId        { get; set; }

        [JsonProperty(PropertyName = __TAG_30651, Order = 15)]
        public string HaltRsnCode        { get; set; }

        [JsonProperty(PropertyName = __TAG_20009, Order = 16)]
        public string ProductID        { get; set; }

    }
}
