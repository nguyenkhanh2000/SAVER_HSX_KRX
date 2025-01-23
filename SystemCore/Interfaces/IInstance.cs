using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Interfaces
{
    /// <summary>
    /// 
    /// </summary>
    public interface IInstance
    {
        /// <summary>
        /// 2019-01-09 10:24:56 ngocta2
        /// id de nhan biet instance cu hay moi
        /// => "3e696ccb-cbe2-4795-b3af-3f65b990b4a1"
        /// </summary>
        string InstanceId { get; }

        /// <summary>
        /// 2019-01-09 12:35:38 ngocta2
        /// thoi diem tao instance
        /// => "2019-01-09 12:35:38"
        /// </summary>
        string CreatedDateTime { get; }
    }
}
