using System;
using SystemCore.Entities;
using SystemCore.Interfaces;

namespace SystemCore.SharedKernel
{
	/// <summary>
	/// 2019-01-09 13:35:39 ngocta2
	/// dung de phan biet cac instance
	/// </summary>
	public abstract class CInstance : IInstance
    {
        // private vars 
        private string _instanceId;
        private string _createdDateTime;

        // public props 
        public string InstanceId { get { return this._instanceId; } }

        public string CreatedDateTime { get { return this._createdDateTime; } }

        /// <summary>
        /// constructor
        /// </summary>
        public CInstance()
        {
            this._instanceId = Guid.NewGuid().ToString();
            this._createdDateTime = DateTime.Now.ToString(EGlobalConfig.DATETIME_MONITOR);
        }
    }
}
