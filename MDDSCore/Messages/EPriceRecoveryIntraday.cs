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
	public class EPriceRecoveryIntraday : EBase
    {
		/// <summary>
		/// W = Price Recovery(Bond) 
		/// </summary>
		public const string __MSG_Type = "W";

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
		[JsonProperty(PropertyName = "30001", Order = 8)]
		public string MarketID { get; set; }

		/// <summary>
		/// 2019-12-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20004; required=Y; format=String; length=2</i></para>
		/// <para><b>ID Bảng giao dịch</b></para>
		/// <para>
		/// G1 : Chính(Main)<br></br>
		/// G2 : Trước giờ giao dịch(Pre Open)<br></br>
		/// G3 : Sau giờ giao dịch(Post Close)<br></br>
		/// G4 : Lô lẻ(Odd lot)<br></br>
		/// G7 : Mua bắt buộc(Buy-in)<br></br>
		/// G8 : Bán bắt buộc(Sell-out)<br></br>
		/// T1 : Thỏa thuận(regular)<br></br>
		/// T4 : Thỏa thuận lô lẻ(regular for Odd lot)<br></br>
		/// T2 : Thỏa thuận trước giờ giao dịch(pre)<br></br>
		/// T3 : Thỏa thuận sau giờ giao dịch(post)<br></br>
		/// T5 : Thỏa thuận sau giờ giao dịch cho lô lẻ(post for Odd lot)<br></br>
		/// R1 : Thỏa thuận(Repo)<br></br>
		/// AL : Tất cả Bảng giao dịch<br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = "20004", Order = 9)]
		public string BoardID { get; set; }

		/// <summary>
		/// 2020-04-27 15:11:08 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=336 ; required=Y; format=String; length=2</i></para>
		/// <para>
		///01 = Nạp lại Lệnh GT <br></br>
		///10 = Phiên mở cửa <br></br>
		///11 = Phiên mở cửa (mở rộng) <br></br>
		///20 = Phiên định kỳ sau khi dừng thị trường <br></br>
		///21 = Phiên định kỳ sau khi dừng thị trường (mở rộng) <br></br>
		///30 = Kết thúc phiên định kỳ <br></br>
		///40 = Phiên liên tục <br></br>
		///50 = Kiểm soát biến động giá <br></br>
		///51 = Kiểm soát biến động giá (mở rộng) <br></br>
		///60 = Tiếp nhận giá đóng cửa sau khi đóng cửa <br></br>
		///80 = Phiên khớp lệnh định kỳ nhiều lần <br></br>
		///90 = Tạm ngừng giao dịch <br></br>
		///99 = Đóng cửa thị trường <br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = "336", Order = 10)]
		public string TradingSessionID { get; set; }

		/// <summary>
		/// 2020-04-27 15:11:08 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=55 ; required=Y; format=String; length=12</i></para>
		/// <para><b>Mã ISIN , trong trường hợp là sản phẩm REPO thì do Sở định nghĩa. </b></para>
		/// </summary>
		[JsonProperty(PropertyName = "55", Order = 11)]
		public string Symbol { get; set; }

		/// <summary>
		/// 2020-04-27 15:12:40 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30561 ; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá mở cửa</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30561", Order = 12)]
		public double OpnPx { get; set; }

		/// <summary>
		/// 2020-04-27 15:12:40 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30562 ; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá cao nhất trong suốt phiên giao dịch của ngày hiện tại</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30562", Order = 13)]
		public double TrdSessnHighPx { get; set; }

		/// <summary>
		/// 2020-04-27 15:12:40 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30563 ; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá thấp nhất trong suốt phiên giao dịch của ngày hiện tại</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30563", Order = 14)]
		public double TrdSessnLowPx { get; set; }

		/// <summary>
		/// 2020-04-27 15:12:40 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20026  ; required=Y; format=Price; length=15(9.4)</i></para>
		/// <para><b>Giá thực hiện cuối cùng của ngày đó. Nếu không có thực hiện, nó sẽ được thay thế bằng giá tham chiếu và nếu không có giá tham chiếu thì nó sẽ là 0 (không)  </b></para>
		/// </summary>
		[JsonProperty(PropertyName = "20026", Order = 15)]
		public double SymbolCloseInfoPx { get; set; }

		/// <summary>
		/// 2020-04-27 15:13:36 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30565  ; required=Y; format=Float; length=13(5.6)</i></para>
		/// <para><b>Tỷ suất giá mở cửa trái phiếu</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30565", Order = 16)]
		public double OpnPxYld { get; set; }

		/// <summary>
		/// 2020-04-27 15:13:36 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30566  ; required=Y; format=Float; length=13(5.6)</i></para>
		/// <para><b>Lợi tức cao nhất trái phiếu trong suốt phiên giao dịch của ngày hiện tại</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30566", Order = 17)]
		public double TrdSessnHighPxYld { get; set; }

		/// <summary>
		/// 2020-04-27 15:13:36 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30567  ; required=Y; format=Float; length=13(5.6)</i></para>
		/// <para><b>Lợi tức thấp nhất trái phiếu trong suốt phiên giao dịch của ngày hiện tại</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30567", Order = 18)]
		public double TrdSessnLowPxYld { get; set; }

		/// <summary>
		/// 2020-04-27 15:13:36 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30568  ; required=Y; format=Float; length=13(5.6)</i></para>
		/// <para><b>Lợi tức đóng cửa trái phiếu</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30568", Order = 19)]
		public double ClsPxYld { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=387 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng giao dịch lũy kế trong ngày</b></para>
		/// </summary
		[JsonProperty(PropertyName = "387", Order = 20)]
		public long TotalVolumeTraded { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=381 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Tổng giá trị giao dịch lũy kế trong ngày</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "381", Order = 21)]
		public double GrossTradeAmt { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30521 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng của các lệnh bên bán</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30521", Order = 22)]
		public long SellTotOrderQty { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30522 ; required=Y; format=Int; length=12</i></para>
		/// <para><b>Tổng khối lượng của các lệnh bên mua</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30522", Order = 23)]
		public long BuyTotOrderQty { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30523 ; required=Y; format=Int; length=11</i></para>
		/// <para><b>Số lượng chào giá hợp lệ bên bán</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30523", Order = 24)]
		public long SellValidOrderCnt { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30524 ; required=Y; format=Int; length=11</i></para>
		/// <para><b>Số lượng chào giá hợp lệ bên mua</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30524", Order = 25)]
		public long BuyValidOrderCnt { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=268 ; required=Y; format=Int; length=9</i></para>
		/// <para><b>Số lần lặp dữ liệu được thực hiện ở nội dung bên dưới(Market depth).</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "268", Order = 26)]
		public long NoMDEntries { get; set; }

		/// <summary>
		/// 2020-08-27 09:27:52 ngocta2
		/// field nay tu nghi ra, ko co trong spec MDDS, nen ko co tag		
		/// <para><i>field nay tu nghi ra, ko co trong spec MDDS, nen ko co tag</i></para>
		/// <para><i>yeu cau do dai [varchar](max) hoac 3000 ky tu cua field trong db MSSQL/Oracle</i></para>
		/// <para><b>lay tat ca data tu 8=FIX.4.4 cho den het, gom ca ky tu ko nhin thay dc</b></para>
		/// </summary>
		public string RepeatingDataFix { get; set; }

		/// <summary>
		/// 2020-08-27 09:27:52 ngocta2
		/// field nay tu nghi ra, ko co trong spec MDDS, nen ko co tag		
		/// <para><i>field nay tu nghi ra, ko co trong spec MDDS, nen ko co tag</i></para>
		/// <para><i>yeu cau do dai [varchar](max) hoac 3000 ky tu cua field trong db MSSQL/Oracle</i></para>
		/// <para><b>day la json data (JSON format), co the query theo struct (MSSQL 2016+)</b></para>
		/// <para>{"data":[{"83":"1","270":"aaa"},{"83":"2","270":"bbbbb"}]}</para>
		/// </summary>
		public string RepeatingDataJson { get; set; }
	}
}
