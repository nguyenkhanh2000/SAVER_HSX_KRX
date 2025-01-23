using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.TAChart.Entities
{
    /// <summary>
    /// 2019-01-16 09:41:43 ngocta2
    //https://stackoverflow.com/questions/454916/performance-of-arrays-vs-lists
    //https://stackoverflow.com/questions/10018957/how-to-remove-item-from-list-in-c
    //// su dung ConcurrentDictionary
    //dic["ACB"] = List<object>
    //object.UpdateDate  // DateTime.Now
    //object.JsonA.ServerTime  // "f399": "13:02:39",
    //object.JsonA.MatchPrice  // "f31": "12000",
    //object.JsonA.MatchQuantity  // "f32": "500",
    /// </summary>
    public class EJsonAData
    {
        /// <summary>
        /// thoi diem update data
        /// dung field nay de auto clear data qua cu
        /// de nhieu ele trong list lam giam speed
        /// </summary>
        [JsonProperty(PropertyName = "UD")]
        public DateTime UpdateDate { get; }


        /// <summary>
        /// chi tiet thong tin jsonA
        /// </summary>
        [JsonProperty(PropertyName = "D")]
        public EJsonADetail Detail { get; set; }


        /// <summary>
        /// constructor
        /// </summary>
        public EJsonAData()
        {
            this.UpdateDate = DateTime.Now;
        }
    }
}
