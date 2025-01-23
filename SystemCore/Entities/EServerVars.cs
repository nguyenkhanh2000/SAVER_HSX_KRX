using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
	/// <summary>
	/// 2019-11-26 15:33:57 ngocta2
	/// luu cac info thuoc ServerVariables
	/// HttpContext.Current.Request.ServerVariables["HTTP_USER_AGENT"].ToString()
	/// </summary>
	public class EServerVars
	{
		public string RemoteAddr { get; set; } // ClientIp 
		public string LocalAddr { get; set; } // ServerIp 
		public string HttpUserAgent { get; set; } // HTTP_USER_AGENT
	}
}
