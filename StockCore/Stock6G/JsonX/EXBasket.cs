using Newtonsoft.Json;
using System.Runtime.Serialization;

namespace StockCore.Stock6G.JsonX
{
    /// <summary>
    /// 2020-08-17 15:04:08 ngocta2
    /// struct gia tham chieu, tran, san
    /// giong 5G https://liveprice.fpts.com.vn/hsx/data.ashx?s=basket
    /// </summary>
    [JsonObject(MemberSerialization.OptIn)]
    public class EXBasket: EXBase
	{
        /// <summary>
        /// 
        /// 2020-08-17 14:59:15 ngocta2
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>Msg=ML; Field=IndexsTypeCode; Tag=30167</i></para>
        /// <para><b>Mã của chỉ số được tính và công bố trên thị trường bao gồm chỉ số ngành, chỉ số đại diện thị trường, ví dụ VN Index, VN30, VNAllshares…</b></para>
		/// </summary>
		[JsonProperty(PropertyName = __SHORT_NAME, Order = 1)]
        [DataMember(Name = __SHORT_NAME)]
        public string Name { get; set; }

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
        [JsonProperty(PropertyName = __SHORT_MARKETID, Order = 2)]
        [DataMember(Name = __SHORT_MARKETID)]
        public string MarketID { get; set; }

        /// <summary>
        /// 2020-08-17 15:01:57 ngocta2
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// danh muc ma chung khoan
        /// </summary>
        [JsonProperty(PropertyName = __SHORT_LIST, Order = 3)]
        [DataMember(Name = __SHORT_LIST)]
        public string List { get; set; }

        ///// <summary>
        /////"Mã thị trường của một chỉ số**Code**
        /////1: HoSE
        /////2: HNX
        /////3: Chỉ số chung(HoSE + HNX)"
        ///// </summary>
        //[JsonProperty(PropertyName = __SHORT_CLASS, Order = 4)]
        //[DataMember(Name = __SHORT_CLASS)]
        //public string Class { get; set; }

        /// <summary>
        /// Tên của chỉ sô (VN)
        /// </summary>
        [JsonProperty(PropertyName = __SHORT_NAMEVN, Order = 5)]
        [DataMember(Name = __SHORT_NAMEVN)]
        public string NameVN { get; set; }

        /// <summary>
        /// Tên của chỉ sô (EN)
        /// </summary>
        [JsonProperty(PropertyName = __SHORT_NAMEEN, Order = 6)]
        [DataMember(Name = __SHORT_NAMEEN)]
        public string NameEN { get; set; }

        ///// <summary>
        ///// Tổng số chứng khoán thuộc rổ chỉ sô
        ///// </summary>
        //[JsonProperty(PropertyName = __SHORT_TOTAL, Order = 7)]
        //[DataMember(Name = __SHORT_TOTAL)]
        //public long Total { get; set; }

        /// <summary>
        /// 2022-03-08 them truong nay de viet tat name, data lay trong config
        /// ten viet tat, dai qua hon 10 ky tu lam hong UI
        /// </summary>
        [JsonProperty(PropertyName = __SHORT_SYMBOL, Order = 8)]
        [DataMember(Name = __SHORT_SYMBOL)]
        public string Short { get; set; }
    }
}
