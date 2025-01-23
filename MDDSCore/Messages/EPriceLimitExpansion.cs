using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=22; name=Price Limit Expansion; type=MX; Online</i></para>
	/// <para><b>Phân phối thông tin mở rộng về giới hạn giá khi xảy ra do kiểm soát thị trường</b></para>
	/// <para>Thông tin này được phân phối trong giờ giao dịch mỗi khi có sự thay đổi mức giá giới hạn của sản phẩm phái sinh bởi người vận hành thị trường. </para>   
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EPriceLimitExpansion :EBase
    {

		/// <summary>
		/// MX = Price Limit Expansion
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_PRICE_LIMIT_EXPANSION;
        public EPriceLimitExpansion(string rawData) : base(rawData)
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
		/// 2020-04-27 15:11:08 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55 , Order = 10)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=60 ; required=Y; format=UTCTime; length=9</i></para>
		/// <para><b>Thời gian thực thi HHmmSSsss</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_60 , Order = 11)]
		public string TransactTime { get; set; }

		/// <summary>
		/// 2020-04-27 15:11:08 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=1149; required=Y; format=Price; length=15(9.4)  </i></para>
		/// <para><b>Giá trần </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_1149 , Order = 12)]
		public double HighLimitPrice { get; set; }  

		/// <summary>
		/// 2020-04-27 15:12:09 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=1148; required=Y; format=Price; length=15(9.4)  </i></para>
		/// <para><b>Giá sàn </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_1148 , Order = 13)]
		public double LowLimitPrice { get; set; }      


	}
}
