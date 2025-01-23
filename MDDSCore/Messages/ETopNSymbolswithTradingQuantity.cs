using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=26; name=Top N Symbols with Trading Quantity; type=MP; Online</i></para>
	/// <para><b>Phân phối thông tin top N cổ phiếu với khối lượng giao dịch lớn nhất cho mỗi thị trường</b></para>   
	/// <para>Thông tin thống kê này được tạo ra định kỳ trong giờ giao dịch cung cấp Top N chứng khoán có khối lượng giao dịch lớn nhất.Dữ liệu sớm nhất được tạo ra sau đợt khớp lệnh định kỳ mở cửa</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class ETopNSymbolsWithTradingQuantity :EBase
    {

		/// <summary>
		/// MP = Top N Symbols with Trading Quantity
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_TOP_N_SYMBOLS_WITH_TRADING_QUANTITY;


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
		/// 2019-11-28 16:32:45 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=911; required=Y; format=Int; length=16</i></para>
		/// <para><b>Tổng số lượng message thống kê</b></para>
		/// </summary>
	    [JsonProperty(PropertyName = __TAG_911 , Order = 9)]
		public long TotNumReports { get; set; }

		/// <summary>
		/// 2019-11-28 16:32:45 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30634; required=Y; format=Int; length=6</i></para>
		/// <para><b>Thứ hạng của chứng khoán trong bảng dữ liệu của thị trường</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30634 , Order = 10)]
		public long Rank { get; set; }

		/// <summary>
		/// 2019-12-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55 , Order =11)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2019-12-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=271 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Khối lượng</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_271 , Order = 12)]
		public long MDEntrySize { get; set; }

	
	}
}
