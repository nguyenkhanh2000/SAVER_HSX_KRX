using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using SystemCore.Entities;

namespace StockCore.Redis.Entities
{
    /// <summary>
    /// 2019-01-11 17:11:28 ngocta2
    /// </summary>
    public abstract class EBaseValue
    {
        public const string __FULL_TIME = "Time";
        public const string __FULL_DATA = "Data";

        ///// <summary>
        ///// thoi gian update data vao redis (auto)
        ///// </summary>
        ////[JsonProperty(Order = 1)]
        //[DataMember(Name = __FULL_TIME, Order = 1 )]        
        //public string Time { get; set; }

        ///// <summary>
        ///// noi dung data chinh can luu redis
        ///// </summary>
        //[JsonProperty(Order = 2)]
        //[DataMember(Name = __FULL_DATA, Order = 2)]
        //public object Data { get; set; }

        ///// <summary>
        ///// constructor
        ///// </summary>
        //public EBaseValue()
        //{
        //    this.Time = DateTime.Now.ToString(EGlobalConfig.DATETIME_REDIS_VALUE);
        //}
    }
}
