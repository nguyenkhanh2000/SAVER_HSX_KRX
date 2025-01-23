using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock6G.Entities
{
    [JsonObject(MemberSerialization.OptIn)]
    public class ELs
    {
        [JsonProperty(PropertyName = "ST", Order = 10)]
        public string ST { get; set; }
        [JsonProperty(PropertyName = "MP", Order = 20)]
        public double MP { get; set; }
        [JsonProperty(PropertyName = "MQE", Order = 30)]
        public int MQE { get; set; }
        [JsonProperty(PropertyName = "NMQ", Order = 40)]
        public int NMQ { get; set; }
    }
    public class ETradeDate
    {
        [JsonProperty(PropertyName = "td", Order = 10)]
        public string TD { get; set; }
    }
    public class ElsScocre
    {
        public List<ELs> ListData;
        public string MaxScore;
    }


}
