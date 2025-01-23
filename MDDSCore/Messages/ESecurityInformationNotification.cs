using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=3; name=Security Information Notification; type=M7; Online</i></para>
	/// <para><b>Phân phối thông tin kiểm soát thị trường về việc xác định giá tham chiếu hoặc thay đổi giới hạn giá tự động</b></para>   
	/// <para>Thông tin này được tạo ra mỗi khi có sự thay đổi hoặc cập nhật giá tham chiếu, giá trần, hoặc giá sàn của chứng khoán bởi người vận hành thị trường</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class ESecurityInformationNotification : EBase
    {
		// <summary>
		/// M7 = Security Information Notification
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_SECURITY_INFORMATION_NOTIFICATION;

        public ESecurityInformationNotification()
        {
        }

        public ESecurityInformationNotification(string rawData) : base(rawData)
        {
        }
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
		/// 2019-12-10 15:47:07 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20013; required=N; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá tham chiếu là mức giá cơ sở để tính giá trần/giá sàn </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20013 , Order = 11)]
		public double ReferencePrice { get; set; }  

		/// <summary>
		/// 2019-12-10 15:45:26 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=1149; required=N; format=Price; length=15(9.4)  </i></para>
		/// <para><b>Giá trần </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_1149 , Order = 12)]
		public double HighLimitPrice { get; set; }  

		/// <summary>
		/// 2019-12-10 15:45:40 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=1148; required=N; format=Price; length=15(9.4)  </i></para>
		/// <para><b>Giá sàn </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_1148 , Order = 13)]
		public double LowLimitPrice { get; set; }  

		/// <summary>
		/// 2019-12-10 15:47:36 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20014; required=N; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá xác định cho việc định giá. Biên độ giá dựa trên giá này đến lần khớp lệnh đầu tiên. Khi khớp lệnh lần đầu xảy ra, biên độ giá sẽ được xác định lại trên giá khớp đầu tiên. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20014 , Order = 14)]
		public double EvaluationPrice { get; set; }  

		/// <summary>
		/// 2019-12-10 15:47:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20015 ; required=N; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá đặt cao nhất của việc định giá phiên mở cửa </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20015 , Order = 15)]
		public double HgstOrderPrice { get; set; }  

		/// <summary>
		/// 2019-12-10 15:47:55 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20016 ; required=N; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá đặt thấp nhất của việc định giá phiên mở cửa </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20016 , Order = 16)]
		public double LwstOrderPrice { get; set; }  

		/// <summary>
		/// 2019-12-10 15:47:55 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20020  ; required=N; format=Int ; length=16</i></para>
		/// <para><b>KLCP niêm yết </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20020 , Order = 17)]
		public long ListedShares { get; set; }

		/// <summary>
		/// 2019-12-10 15:51:26 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20018  ; required=N; format=String; length=2 </i></para>
		/// <para><b>Giao dịch không hưởng quyền </b></para>
		/// <para>
		///00: Không có <br></br>
		///01: Chia cổ phiếu/Cổ phiếu thưởng <br></br>
		///02: Chia cổ tức tiền mặt. <br></br>
		///04: Phát hành quyền <br></br>
		///03: Chia cổ phiếu/Cổ phiếu thưởng + Chia cổ tức tiền mặt. <br></br>
		///05: Phát hành quyền + Chia cổ phiếu/Cổ phiếu thưởng <br></br>
		///06:  Phát hành quyền + Chia cổ tức tiền mặt <br></br>
		///07:  Phát hành quyền + Chia cổ phiếu/Cổ phiếu thưởng + Chia cổ tức <br></br>
		///08:  Trái phiếu chuyển đổi <br></br>
		///16: Chia cổ phiếu quỹ. <br></br>
		///P1: Tách cổ phiếu <br></br>
		///P2: Gộp cổ phiếu <br></br>
		///XX: V.v. <br></br>
		///</para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20018 , Order = 18)]
		public string ExClassType { get; set; }

	}
}
