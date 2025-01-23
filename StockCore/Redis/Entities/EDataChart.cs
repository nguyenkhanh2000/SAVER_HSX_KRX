using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Redis.Entities
{
	/// <summary>
	/// 2019-08-06 14:48:49 ngocta2
	/// </summary>
	[JsonObject(MemberSerialization.OptIn)]
	public class EDataChart
	{
		/// <summary>
		/// type
		/// 2019-08-15 15:11:44 ngocta2 rut gon, giam size msg
		/// type => t
		/// </summary>
		[JsonProperty(PropertyName = "t")]
		public string Type { get; set; }

		/// <summary>
		/// key
		/// 2019-08-15 15:11:44 ngocta2 rut gon, giam size msg		/// symbol => s
		/// 2019-08-16 08:29:23 ngocta2 bo not symbol, dung ten kenh lay thong tin ma ck
		/// </summary>
		//[JsonProperty(PropertyName = "s")]
		public string Symbol { get; set; }

		/// <summary>
		/// Value, chua 1 obj con trong day , cu the la EDataSingle
		/// 2019-08-15 15:11:44 ngocta2 rut gon, giam size msg
		/// data => d
		/// </summary>
		[JsonProperty(PropertyName = "d")]
		public object Data { get; set; }
	}
}
