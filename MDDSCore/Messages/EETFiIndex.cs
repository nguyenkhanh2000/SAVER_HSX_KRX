using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=24; name=ETF iIndex; type=MN; Online </i></para>
	/// <para><b>Phân phối iIndex được tính toán theo định kỳ cho mỗi quỹ ETF</b></para>    
	/// <para>Thông tin này được phân phối trong giờ giao dịch cung cấp giá trị của chỉ số iIndex thể hiện biến động giá của chứng chỉ ETF so với chỉ số tham chiếu</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EETFiIndex :EBase
    {
		/// <summary>
		/// MM = ETF iIndex
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_ETF_IINDEX;


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
		/// 2020-04-27 14:56:04 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55, Order = 9)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2020-04-27 14:56:04 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=60 ; required=Y; format=UTCTime; length=9</i></para>
		/// <para><b>Thời gian thực thi HHmmSSsss</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_60, Order = 10)]
		public string TransactTime { get; set; }

		/// <summary>
		/// 2020-04-27 14:56:04 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30217 ; required=Y; format=Float; length=10(6.2)</i></para>
		/// <para><b>Giá trị chỉ số được tính</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30217, Order = 11)]
		public double ValueIndexes { get; set; }  


	}
}
