using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=21; name=Foreigner Order Limit; type=MF; Batch/Online</i></para>
	/// <para><b>Phân phối thông tin về khối lượng chứng khoán được phép giao dịch của nhà đầu tư nước ngoài được thay đổi bởi các lệnh đặt và lệnh khớp của mỗi chứng khoán</b></para>
	/// <para>Thông tin này được phân phối vào đầu ngày cho biết khối lượng chứng khoán nhà đầu tư nước ngoài được phép mua theo mỗi mã chứng khoán, thông tin này cũng được cập nhật trong giờ giao dịch khi lệnh đặt/hoặc lệnh khớp được thực hiện</para>   
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EForeignerOrderLimit : EBase
    {
		/// <summary>
		/// MF = Foreigner Order Limit
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_FOREIGNER_ORDER_LIMIT;


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
		/// 2020-04-27 15:01:41 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55, Order = 9)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2020-04-27 15:01:41 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30557 ; required=Y; format=Int; length=15</i></para>
		/// <para><b>Tổng room nước ngoài</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30557, Order = 10)]
		public long ForeignerBuyPosblQty { get; set; }

		/// <summary>
		/// 2020-04-27 15:01:41 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30558 ; required=Y; format=Int; length=16</i></para>
		/// <para><b>Room còn lại của nhà đầu tư nước ngoài</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30558, Order = 11)]
		public long ForeignerOrderLimitQty { get; set; }

		
	}
}
