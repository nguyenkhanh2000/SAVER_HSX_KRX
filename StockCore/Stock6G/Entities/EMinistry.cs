using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock6G.Entities
{
    /// <summary>
    /// 2021-03-31 14:26:41 ngocta2
    /// struct nay dung chung cho ca company + cw
    /// </summary>
    [JsonObject(MemberSerialization.OptIn)]
    public class EMinistry
    {
        [JsonProperty(PropertyName = "id", Order = 10)]
        public int ID { get; set; }
        [JsonProperty(PropertyName = "vn", Order = 20)]
        public string MinistryName { get; set; }
        [JsonProperty(PropertyName = "en", Order = 30)]
        public string MinistryNameEN { get; set; }
        [JsonProperty(PropertyName = "list", Order = 40)]
        public string SymbolList { get; set; }
        
    }
}
