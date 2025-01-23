using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using SystemCore.Entities;

namespace StockCore.Redis.Entities
{
    public class EDataSingleT<T>: EBaseValue
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
        public T Data { get; set; }
               
        /// <summary>
        /// constructor
        /// </summary>
        public EDataSingleT(T data): base()
        {
            Data = data;
        }
    }
}
