using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=7; name=Symbol Event; type=MI; Batch</i></para>
	/// <para><b>Phân phối thông tin sự kiện doanh nghiệp của mỗi cổ phiếu và lý do sự kiện doanh nghiệp</b></para>   
	/// <para>Thông tin này được tạo ra khi có sự thay đổi trạng thái của chứng khoán bởi người vận hành thị trường.</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class ESymbolEvent : EBase
    {
		/// <summary>
		/// MI = Symbol Event
		/// </summary>
		public const string __MSG_TYPE = __MSG_TYPE_SYMBOL_EVENT;


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
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_55 , Order = 9)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2020-04-27 14:54:26 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30575  ; required=Y; format=String; length=2</i></para>
		/// <para><b>Mã sự kiện như ngừng giao dịch, thanh khoản thấp và v.v ... Mã số 91 (Trading Halt for Repo) chỉ được sử dụng trong hệ thống giao dịch Gateway Repo chứ không phải hệ thống giao dịch Terminal Repo</b></para>
		/// <para>
		///01: Tạm ngừng giao dịch <br></br>		///04: Giao dịch thanh lý <br></br>		///91: Tạm ngừng giao dịch dựa trên tài sản cầm cố <br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30575 , Order = 10)]
		public string EventKindCode { get; set; }

		/// <summary>
		/// 2020-04-27 14:54:26 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30576  ; required=Y; format=String; length=2</i></para>
		/// <para><b>Mã lý do của sự kiện xảy ra cho mỗi trường hợp</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30576 , Order = 11)]
		public string EventOccurrenceReasonCode { get; set; }

		/// <summary>
		/// 2020-04-27 14:54:26 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30577  ; required=Y; format=LocalMkt Date; length=8</i></para>
		/// <para><b>Ngày Bắt đầu/Mở/ Áp dụng</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30577 , Order = 12)]
		public object EventStartDate { get; set; }

		/// <summary>
		/// 2020-04-27 14:48:08 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30578  ; required=Y; format=LocalMkt Date; length=8</i></para>
		/// <para><b>Ngày hết hạn/ ngày kết thúc</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30578 , Order = 13)]
		public object EventEndDate { get; set; }


    }
}
