using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock6G.Entities
{
    public class EBasket
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
        /// ten ro
        /// </summary>
        [JsonProperty(PropertyName = "n", Order = 20)]
        public string Name { get; set; }

        /// <summary>
        /// ten ro
        /// </summary>
        [JsonProperty(PropertyName = "l", Order = 30)]
        public List<string> List { get; set; }

    }
}
