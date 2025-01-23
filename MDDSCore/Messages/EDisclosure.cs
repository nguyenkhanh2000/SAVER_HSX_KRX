using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=31; name=Disclosure; type=MU; Batch/Online</i></para>
	/// <para><b>Phân phối công bố thông tin</b></para>   
	/// <para>Thông tin này cung cấp thông tin công bố của tổ chức niêm yết.</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EDisclosure : EBase
    {
		/// <summary>
		/// MU = Disclosure
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_DISCLOSURE;


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
		/// 2020-04-28 14:10:15 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=207; required=N; format=String; length=2</i></para>
		/// <para><b>Mã sàn giao dịch</b></para>
		/// <para>
		/// HO: HoSE<br></br>
		/// HX: HNX
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_207, Order = 9)]
		public string SecurityExchange { get; set; }

		/// <summary>
		/// 2020-04-28 14:10:15 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55; required=N; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa.</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55, Order = 10)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2020-04-28 14:10:15 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30629; required=N; format=String; length=120</i></para>
		/// <para><b>Tên chứng khoán.</b></para>        
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30629, Order = 11)]
		public string SymbolName { get; set; }

		/// <summary>
		/// 2020-04-28 14:10:15 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30605; required=Y; format=String; length=6</i></para>
		/// <para><b>Mã công bố thông tin</b></para>        
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30605, Order = 12)]
		public string DisclosureID { get; set; }

		/// <summary>
		/// 2020-04-28 14:10:15 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30606  ; required=Y; format=Int; length=16 </i></para>
		/// <para><b>Tên bằng tiếng Anh của chỉ số.</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30606, Order = 13)]
		public long TotalMsgNo { get; set; }

		/// <summary>
		/// 2020-04-28 14:10:15 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30607  ; required=Y; format=Int; length=16 </i></para>
		/// <para><b>Số thứ tự của cổ phiếu trong rổ chỉ số.</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30607, Order = 14)]
		public long CurrentMsgNo { get; set; }

		/// <summary>
		/// 2020-04-28 14:10:15 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30608; required=Y; format=String; length=1</i></para>
		/// <para><b>Giá trị của mã loại cho ngôn ngữ:</b></para>
		/// <para>
		/// 1: tiếng việt<br></br>
		/// 2: tiếng anh<br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30608, Order = 15)]
		public string LanquageCategory { get; set; }

		/// <summary>
		/// 2020-04-28 14:10:15 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30609; required=Y; format=String; length=1</i></para>
		/// <para><b>Dữ liệu được phân phối theo:</b></para>
		/// <para>
		/// 1: Batch<br></br>
		/// 2: Realtime<br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30609, Order = 16)]
		public string DataCategory { get; set; }

		/// <summary>
		/// 2020-04-28 14:29:48 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30610 ; required=Y; format=LocalMkt Date; length=8</i></para>
		/// <para><b>Ngày công bố YYYYMMDD</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30610, Order = 17)]
		public string PublicInformationDate { get; set; }

		/// <summary>
		/// 2020-04-28 14:29:48 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30611 ; required=Y; format=LocalMkt Date; length=8</i></para>
		/// <para><b>Ngày cung cấp YYYYMMDD</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30611, Order = 18)]
		public string TransmissionDate { get; set; }

		/// <summary>
		/// 2020-04-28 14:10:15 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30612; required=Y; format=String; length=1</i></para>
		/// <para><b>Thông tin công bố:</b></para>
		/// <para>
		/// 1: Bình thường<br></br>
		/// 2: Sửa đổi<br></br>
		/// 3: Đã xóa<br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30612, Order = 19)]
		public string ProcessType { get; set; }

		/// <summary>
		/// 2020-04-28 14:10:15 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=148; required=Y; format=String; length=250</i></para>
		/// <para><b>TEXT Format</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_148, Order = 20)]
		public string Headline { get; set; }

		/// <summary>
		/// 2020-04-28 14:10:15 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30613; required=Y; format=String; length=1000</i></para>
		/// <para><b>Nội dung công bô</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30613, Order = 21)]
		public string Body { get; set; }

	}
}
