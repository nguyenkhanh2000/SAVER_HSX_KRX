using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	/// <summary>
	/// 2020-08-03 hungtq
	/// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
	/// <para><i>no=15; name=Investor per Industry (Bond) ; type=M2; online</i></para>
	/// <para><b> Phân phối thông tin thông kê giao dịch theo loại nhà đầu tư của từng loại trái phiếu.</b></para>    
	/// <para>Thông tin thống kê này được tạo ra định kỳ trong giờ giao dịch cung cấp thống kê giao dịch của loại nhà đầu tư theo từng ngành.Dữ liệu sớm nhất được tạo ra sau đợt khớp lệnh định kỳ mở cửa.</para>
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EInvestorPerIndustryBond: EBase
    {
		/// <summary>
		/// M2 = Investor per Industry (Bond)
		/// </summary>
		public const string __MSG_Type = "M2";
		public const string __HNX_INDUSTRY_BOND = "BTX";
		public const string __HCX_INDUSTRY_BOND = "HCX";
		public const string __HSX_INDUSTRY_BOND = "BTO";




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
		[JsonProperty(PropertyName = "30001", Order = 8)]
		public string MarketID { get; set; }

		/// <summary>
		/// 2020-04-27 15:04:38 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=60 ; required=Y; format=UTCTime; length=9</i></para>
		/// <para><b>Thời gian thực thi HHmmSSsss</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "60", Order = 9)]
		public string TransactTime { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:10 hungtq 
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30597  ; required=Y; format=String; length=10 </i></para>
		/// <para><b>Mã phân loại trái phiếu: T-bills, T-Bonds, Note of the central bank, Trái phiếu cầm cố nhà nước … ## Nếu mã CFI đã có chức năng phân loại như trên thì mã phân loại chứng khoán nêu trên không cần thiết. </b></para>
		/// <para>
		///</para>
		/// </summary>
		[JsonProperty(PropertyName = "30597", Order = 10)]
		public string BondClassificationCode { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:10 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=20000  ; required=Y; format=String; length=4 </i></para>
		/// <para><b>Mã phân loại nhà đầu tư</b></para>
		/// <para>
		/// 1000 = Công ty chứng khoán và công ty Futures<br></br>
		/// 2000 = Công ty bảo hiểm<br></br>
		/// 3000 = Công ty quản lý tài sản và Công ty đầu tư<br></br>
		/// 3200 = Quỹ tư nhân<br></br>
		/// 4000 = Ngân hàng(Tài sản tin cậy của công ty quản lý tài sản sẽ được phân loại vào công ty quản lý tài sản)<br></br>
		/// 5000 = Công ty tài chính chung, Ngân hàng tiết kiệm Tương hỗ<br></br>
		/// 6000 = Trợ cấp, Quỹ và Hỗ trợ tương trợ<br></br>
		/// 7000 = quốc gia, tổ chức tự quản<br></br>
		/// 7030 = Tổ chức quốc tế<br></br>
		/// 7100 = Hợp đồng khác<br></br>
		/// 8000 = Cá nhân<br></br>
		/// </para>
		/// </summary>
		[JsonProperty(PropertyName = "20000", Order = 11)]
		public string InvestCode { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=331 ; required=Y; format=Int; length=10</i></para>
		/// <para><b>Khối lượng giao dịch bên bán</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "331", Order = 12)]
		public long SellVolume { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30168 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Giá trị giao dịch bên bán</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30168", Order = 13)]
		public double SellTradeAmount { get; set; }   

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=330 ; required=Y; format=Int; length=10</i></para>
		/// <para><b>Khối lượng giao dịch bên mua</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "330", Order = 14)]
		public long BuyVolume { get; set; }

		/// <summary>
		/// 2020-04-10 11:58:50 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30169 ; required=Y; format=Float; length=23(18.4)</i></para>
		/// <para><b>Giá trị giao dịch bên mua</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "30169", Order = 15)]
		public double BuyTradedAmount { get; set; }   

		
	}
}
