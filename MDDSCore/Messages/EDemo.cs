using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDDSCore.Messages
{
	[JsonObject(MemberSerialization.OptIn)]
	public class EDemo
	{
		[JsonProperty(PropertyName = "TypeBool", Order = 10)]
		public bool TypeBool { get; set; }

		[JsonProperty(PropertyName = "TypeString", Order = 20)]
		public string TypeString { get; set; }

		[JsonProperty(PropertyName = "TypeInt", Order = 30)]
		public int TypeInt { get; set; }

		[JsonProperty(PropertyName = "TypeLong", Order = 40)]
		public long TypeLong { get; set; }

		[JsonProperty(PropertyName = "TypeDecimal", Order = 50)]
		public decimal TypeDecimal { get; set; }

		//[JsonIgnore]
		public DateTime TypeDateTime { get; set; }

		//[JsonIgnore]		
		public TimeSpan TypeTime { get; set; }
		
	}
}
