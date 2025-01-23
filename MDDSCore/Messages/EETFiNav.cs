using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=23; name=ETF iNav; type=MM; Online </i></para>
	/// <para><b>Phân phối thông tin iNav tính toán theo định kỳ cho mỗi quỹ ETF</b></para>    
	/// <para>Thông tin này được phân phối trong giờ giao dịch, cung cấp giá trị tài sản ròng tham chiếu của mỗi chứng chỉ ETF</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EETFiNav :EBase
    {
		/// <summary>
		/// MM = ETF iNav
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_ETF_INAV;


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
		/// <para><i>tag=60 ; required=Y; format=UTCTime; length=9</i></para>
		/// <para><b>Thời gian thực thi HHmmSSsss</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_60, Order = 10)]
		public string TransactTime { get; set; }

		/// <summary>
		/// 2020-04-27 14:56:31 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30599 ; required=Y; format=Float; length=24(18.4)</i></para>
		/// <para><b>Giá trị iNAV được tính</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30599 , Order = 11)]
		public double iNAVvalue { get; set; }  

	}
}
