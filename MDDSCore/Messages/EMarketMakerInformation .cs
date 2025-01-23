using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=6; name=Market Maker Information; type=MH; Batch</i></para>
	/// <para><b>Phân phối thông tin của nhà tạo lập thị trường theo từng mã cổ phiếu/trái phiếu hoặc từng sản phẩm đối với thị trường phái sinh</b></para>   
	/// <para>Thông tin về Nhà tạo lập thị trường được cung cấp vào đầu ngày cho các chứng khoán có Nhà tạo lập thị trường.</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EMarketMakerInformation :EBase
    {

		/// <summary>
		/// MH = Market Maker Information
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_MARKET_MAKER_INFORMATION;


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
		/// 2019-12-11 14:59:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30574  ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN  của chứng khoán có nhà tạo lập thị trường. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30574 , Order = 9)]
		public string MarketMakerContractCode { get; set; }

		/// <summary>
		/// 2019-12-11 15:19:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20023   ; required=Y; format=String; length=5</i></para>
		/// <para><b>Mã thành viên (giống như  Mã thành viên tham gia thị trường) </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20023 , Order = 10)]
		public string MemberNo { get; set; }

	}
}
