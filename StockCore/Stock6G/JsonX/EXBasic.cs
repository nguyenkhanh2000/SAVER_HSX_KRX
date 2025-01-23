using Newtonsoft.Json;

namespace StockCore.Stock6G.JsonX
{
	/// <summary>
	/// 2020-08-17 15:04:08 ngocta2
	/// struct gia tham chieu, tran, san
	/// giong 5G https://liveprice.fpts.com.vn/hsx/data.ashx?s=basic
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EXBasic
	{

		/// <summary>
		/// 2020-08-17 14:59:15 ngocta2
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=30624; required=Y; format=String; length=20</i></para>
		/// <para><b>Mã chứng khoán, (trading code, local code, stock code, bond code và v.v ...)</b></para>
		/// </summary>
		[JsonProperty(PropertyName = "s", Order = 1)]
		public string Symbol { get; set; }

        /// <summary>
        /// 2020-08-17 15:01:15 ngocta2
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
        [JsonProperty(PropertyName = "m", Order = 2)]
        public string MarketID { get; set; }

        /// <summary>
        /// 2020-08-17 15:01:57 ngocta2
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20013; required=Y; format=Price; length=15(9.4)</i></para>
        /// <para><b>Giá tham chiếu là mức giá cơ sở để tính giá trần/giá sàn </b></para>
        /// </summary>
        [JsonProperty(PropertyName = "r", Order = 3)]
        public double ReferencePrice { get; set; }  

        /// <summary>
        /// 2020-08-17 15:03:09 ngocta2
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=1149; required=Y; format=Price; length=15(9.4)  </i></para>
        /// <para><b>Giá trần </b></para>
        /// </summary>
        [JsonProperty(PropertyName = "c", Order = 4)]
        public double CeilingPrice { get; set; }

        /// <summary>
        /// 2020-08-17 15:03:23 ngocta2
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=1148; required=Y; format=Price; length=15(9.4)  </i></para>
        /// <para><b>Giá sàn </b></para>
        /// </summary>
        [JsonProperty(PropertyName = "f", Order = 5)]
        public double FloorPrice { get; set; }    
    }
}
