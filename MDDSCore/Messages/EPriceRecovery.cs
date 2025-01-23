using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=12; name=Price Recovery(Bond) ; type=W; Online</i></para>
	/// <para><b>Phân phối dữ liệu phục hồi về giá (trái phiếu) theo định kỳ(tự động) </b></para>   
	/// <para>Thông tin này là bản chụp trạng thái thị trường được thực hiện định kỳ trong giờ giao dịch cung cấp thông tin về lệnh đặt, lệnh khớp. </para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EPriceRecovery : EBasePrice
	{
		/// <summary>
		/// W = Price Recovery(Bond) 
		/// </summary>
		new public const string __MSG_TYPE = __MSG_TYPE_PRICE_RECOVERY; // override __MSG_TYPE cua EBasePrice

		/// <summary>
		/// 2020-04-27 15:12:40 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30561 ; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá mở cửa</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30561, Order = 12)]
		public double OpnPx { get; set; }  

		/// <summary>
		/// 2020-04-27 15:12:40 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30562 ; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá cao nhất trong suốt phiên giao dịch của ngày hiện tại</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30562, Order = 13)]
		public double TrdSessnHighPx { get; set; }  

		/// <summary>
		/// 2020-04-27 15:12:40 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30563 ; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá thấp nhất trong suốt phiên giao dịch của ngày hiện tại</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30563, Order = 14)]
		public double TrdSessnLowPx { get; set; }  

		/// <summary>
		/// 2020-04-27 15:12:40 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20026  ; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá thực hiện cuối cùng của ngày đó. Nếu không có thực hiện, nó sẽ được thay thế bằng giá tham chiếu và nếu không có giá tham chiếu thì nó sẽ là 0 (không)  </b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_20026, Order = 15)]
		public double SymbolCloseInfoPx { get; set; }  

		/// <summary>
		/// 2020-04-27 15:13:36 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30565  ; required=Y; format=Float; length=13(5.6)</i></para>
		/// <para><b>Tỷ suất giá mở cửa trái phiếu</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30565, Order = 16)]
		public double OpnPxYld { get; set; }  

		/// <summary>
		/// 2020-04-27 15:13:36 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30566  ; required=Y; format=Float; length=13(5.6)</i></para>
		/// <para><b>Lợi tức cao nhất trái phiếu trong suốt phiên giao dịch của ngày hiện tại</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30566, Order = 17)]
		public double TrdSessnHighPxYld { get; set; }  

		/// <summary>
		/// 2020-04-27 15:13:36 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30567  ; required=Y; format=Float; length=13(5.6)</i></para>
		/// <para><b>Lợi tức thấp nhất trái phiếu trong suốt phiên giao dịch của ngày hiện tại</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30567, Order = 18)]
		public double TrdSessnLowPxYld { get; set; }  

		/// <summary>
		/// 2020-04-27 15:13:36 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30568  ; required=Y; format=Float; length=13(5.6)</i></para>
		/// <para><b>Lợi tức đóng cửa trái phiếu</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __TAG_30568, Order = 19)]
		public double ClsPxYld { get; set; }



		/// <summary>
		/// constructor
		/// </summary>
		/// <param name="rawData"></param>
		/// <param name="basePriceInstance"></param>
		public EPriceRecovery(string rawData, EBasePrice basePriceInstance) : base(rawData, basePriceInstance)
		{
		}
	}
}
