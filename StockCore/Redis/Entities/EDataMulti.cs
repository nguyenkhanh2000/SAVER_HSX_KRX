using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using SystemCore.Entities;

namespace StockCore.Redis.Entities
{
    /// <summary>
    /// json = {"Time":"2021-03-25 11:27:01.805","Data":[{"m":"STO","n":"VN30","l":"FPT,SSI,VNM"},{"m":"STO","n":"VN100","l":"ABT,FTS"}]}
    /// EDataMulti<EXBasket> data = Newtonsoft.Json.JsonConvert.DeserializeObject<EDataMulti<EXBasket>>(json);
    //data.Data[0]
    //    {StockCore.Stock6G.JsonX.EXBasket}
    //List: "FPT,SSI,VNM"
    //MarketID: "STO"
    //Name: "VN30"
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class EDataMulti<T> : EBaseValue
    {
        /// <summary>
        /// thoi gian update data vao redis (auto)
        /// </summary>
        [JsonProperty(Order = 1)]
        [DataMember(Name = __FULL_TIME, Order = 1)]
        public string Time { get; set; } = EGlobalConfig.DateTimeNow;

        /// <summary>
        /// noi dung data chinh can luu redis
        /// </summary>
        [JsonProperty(Order = 2)]
        [DataMember(Name = __FULL_DATA, Order = 2)]
        public IList<T> Data { get; set; }

        /// <summary>
        /// constructor
        /// </summary>
        public EDataMulti(IList<T> data) : base()
        {
            Data = data;
        }
    }
}
