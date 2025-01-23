using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
	/// <summary>
	/// 2019-11-27 11:21:29 ngocta2
	/// DTO return tu WebApi (GUI)
	/// </summary>
	public class EResponseResult
	{
		/// <summary>
		/// code return tu sp hoac system neu co exception
		/// </summary>
		public long Code { get; set; }

		/// <summary>
		/// message return tu sp hoac error msg neu co exception
		/// </summary>
		public string Message { get; set; }

		/// <summary>
		/// data khong xac dinh type
		/// </summary>
		public object Data { get; set; }
	}
}
