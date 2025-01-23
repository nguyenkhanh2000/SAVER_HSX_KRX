using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=25; name=ETF TrackingError; type=MO; Batch </i></para>
	/// <para><b>Phân phối thông tin tracking error được tính toán cho mỗi quỹ ETF</b></para>    
	/// <para>Thông tin này được phân phối đầu ngày cung cấp giá trị Tracking Error được tính cho mỗi ETF</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EETFTrackingError :EBase
    {

		/// <summary>
		/// MO = ETF TrackingError
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_ETF_TRACKINGERROR;

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
		/// 2020-04-27 14:56:31 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55, Order = 9)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2020-04-27 14:56:31 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=75 ; required=Y; format=LocalMkt Date; length=8</i></para>
		/// <para><b>Ngày giao dịch. Định dạng (YYYYMMDD)</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_75, Order = 10)]
		public string TradeDate { get; set; }

		/// <summary>
		/// 2020-04-27 14:56:31 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30600 ; required=Y; format=Float; length=13(5.6)</i></para>
		/// <para><b>Giá trị của tracking error </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30600, Order = 11)]
		public double TrackingError { get; set; }  

		/// <summary>
		/// 2020-04-27 14:56:31 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30602 ; required=Y; format=Float; length=24(16.6)</i></para>
		/// <para><b>Giá trị tuyệt đối của tỷ lệ sai lệch của tracking error</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30602, Order = 12)]
		public double DisparateRatio { get; set; }  

	}
}
