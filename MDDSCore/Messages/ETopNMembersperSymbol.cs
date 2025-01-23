using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=18; name=Top N Members per Symbol; type=M4; Online</i></para>
	/// <para><b>Phân phối thông tin top N thành viên cho mỗi cổ phiếu</b></para>   
	/// <para>Thông tin thống kê này được tạo ra định kỳ trong giờ giao dịch cung cấp Top N thành viên theo từng mã chứng khoán. Dữ liệu sớm nhất được tạo ra sau đợt khớp lệnh định kỳ mở cửa.</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class ETopNMembersPerSymbol :EBase
	{
		/// <summary>
		/// M4 = Top N Members per Symbol
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_TOP_N_MEMBERS_PER_SYMBOL;


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
		/// 2019-12-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55 , Order = 9)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2019-11-28 16:32:45 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=911; required=Y; format=Int; length=16</i></para>
		/// <para><b>Tổng số lượng message thống kê</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_911 , Order = 10)]
		public long TotNumReports { get; set; }

		/// <summary>
		/// 2019-11-28 16:32:45 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30215; required=Y; format=Int; length=6</i></para>
		/// <para><b>Số thứ tự xếp hạng trong nhóm bán</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30215 , Order = 11)]
		public long SellRankSeq { get; set; }

		/// <summary>
		/// 2019-11-28 16:32:45 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30213; required=Y; format=String; length=5</i></para>
		/// <para><b>Mã thành viên bên bán</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30213 , Order = 12)]
		public string SellMemberNo { get; set; }

		/// <summary>
		/// 2019-11-28 16:32:45 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=331 ; required=Y; format=Int; length=10</i></para>
		/// <para><b>Khối lượng giao dịch bên bán</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_331 , Order = 13)]
		public long SellVolume { get; set; }

		/// <summary>
		/// 2019-11-28 16:32:45 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30168 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Giá trị giao dịch bên bán</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30168 , Order = 14)]
		public double SellTradeAmount { get; set; }   

		/// <summary>
		/// 2019-11-28 16:32:45 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30216; required=Y; format=Int; length=6</i></para>
		/// <para><b>Số thứ tự xếp hạng trong nhóm mua</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30216 , Order = 15)]
		public long BuyRankSeq { get; set; }

		/// <summary>
		/// 2019-11-28 16:32:45 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30214; required=Y; format=String; length=5</i></para>
		/// <para><b>Mã thành viên bên mua</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30214 , Order = 16)]
		public string BuyMemberNo { get; set; }

		/// <summary>
		/// 2019-11-28 16:32:45 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=330 ; required=Y; format=Int; length=10</i></para>
		/// <para><b>Khối lượng giao dịch bên mua</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_330 , Order = 17)]
		public long BuyVolume { get; set; }

		/// <summary>
		/// 2019-11-28 16:32:45 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30169 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Giá trị giao dịch bên mua</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30169 , Order = 18)]
		public double BuyTradedAmount { get; set; }   

	}
}
