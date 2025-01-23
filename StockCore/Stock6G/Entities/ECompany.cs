using Newtonsoft.Json;

namespace StockCore.Stock6G.Entities
{
    /// <summary>
    /// 2021-03-08 13:38:52 ngocta2
    /// ten cong ty
    /// </summary>
    [JsonObject(MemberSerialization.OptIn)]
    public class ECompany
    {
        /// <summary>
        /// 2020-08-17 15:01:15 ngocta2
        /// market
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
        [JsonProperty(PropertyName = "m", Order = 10)]
        public string MarketID { get; set; }

        /// <summary>
        /// id cua symbol theo FPTS (ezsearch id)
        /// </summary>
        [JsonProperty(PropertyName = "cid", Order = 20)]
        public int CompanyID { get; set; }

        /// <summary>
        /// id cua symbol theo so tra ve, 1 string dai 12 ky tu
        /// VN000000ADS0
        /// </summary>
        [JsonProperty(PropertyName = "sid", Order = 30)]
        public string SymbolID { get; set; }

        /// <summary>
        /// 2020-08-17 14:59:15 ngocta2
        /// symbol
        /// d	TickerCode	30624	CII	VN000000CII6-CII
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30624; required=Y; format=String; length=20</i></para>
        /// <para><b>Mã chứng khoán, (trading code, local code, stock code, bond code và v.v ...)</b></para>
        /// </summary>
        [JsonProperty(PropertyName = "s", Order = 40)]
        public string Symbol { get; set; }

        /// <summary>
        /// ten cong ty theo VN
        /// </summary>
        [JsonProperty(PropertyName = "vn", Order = 50)]
        public string NameVn { get; set; }

        /// <summary>
        /// ten cong ty theo EN
        /// </summary>
        [JsonProperty(PropertyName = "en", Order = 60)]
        public string NameEn { get; set; }
    }
}
