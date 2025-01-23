using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=8; name=Index Constituents Information; type=ML; Batch</i></para>
	/// <para><b>Phân phối danh mục chứng khoán thành phần của mỗi chỉ số	 </b></para>    
	/// <para>Cung cấp thông tin về danh sách chứng khoán thành phần của mỗi chỉ số.</para>
	/// -----
	/// 8=FIX.4.49=17635=ML49=VNMGW56=9999934=7352=20220222 08:00:01.93030001=STO30569=130167=31015=VND30632=VNAllShare Utilities30633=VNAllShare Utilities30606=930607=155=VN000000BTP010=051
	/// 8=FIX.4.49=17035=ML49=VNMGW56=9999934=2252=20211116 08:00:02.10130001=UPX30569=430167=31015=VND30632=UPCOM Large Index30633=UPCOM Large Index30606=5330607=855=VN000000DDV810=104
	/// ----
	/// cung 1 index code nhung khac market class
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EIndexConstituentsInformation :EBase
   {
		/// <summary>
		/// ML = Index Constituents Information
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_INDEX_CONSTITUENTS_INFORMATION;


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
		/// 2020-04-10 11:58:10 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30569  ; required=Y; format=String; length=1 </i></para>
		/// <para><b>Mã thị trường của một chỉ số**Code**</b></para>
		/// <para>
		///1: HoSE<br></br>
		///2: HNX <br></br>
		///3: Chỉ số chung (HoSE + HNX) <br></br>
		///</para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30569 , Order = 9)]
		public string MarketIndexClass { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:10 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30167  ; required=Y; format=String; length=8 </i></para>
		/// <para><b>Mã của chỉ số được tính và công bố trên thị trường bao gồm chỉ số ngành, chỉ số đại diện thị trường, ví dụ VN Index, VN30, VNAllshares…</b></para>
		/// <para>
		///</para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30167 , Order = 10)]
		public string IndexsTypeCode { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:10 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=15  ; required=Y; format=String; length=3 </i></para>
		/// <para><b>Mã tiền theo ISO 4217 VND, USD, EUR</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_15 , Order = 11)]
		public string Currency { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:10 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30632  ; required=Y; format=String; length=120 </i></para>
		/// <para><b>Tên của chỉ sô</b></para>
		/// </summary
		[JsonProperty(PropertyName = __TAG_30632 , Order = 12)]
		public string IdxName { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:10 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30633  ; required=Y; format=String; length=80 </i></para>
		/// <para><b>Tên bằng tiếng Anh của chỉ số.</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30633 , Order = 13)]
		public string IdxEnglishName { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:10 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30606  ; required=Y; format=Int; length=16 </i></para>
		/// <para><b>Tổng số chứng khoán thuộc rổ chỉ số</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30606 , Order = 14)]
		public long TotalMsgNo { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:10 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30607  ; required=Y; format=Int; length=16 </i></para>
		/// <para><b>Số thứ tự của cổ phiếu trong rổ chỉ số.</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30607 , Order = 15)]
		public long CurrentMsgNo { get; set; }

		/// <summary>
		/// 2020-04-27 15:03:42 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55, Order = 16)]
		public string Symbol { get; set; }


	}
}
