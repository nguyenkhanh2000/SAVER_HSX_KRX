using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
	// 2020-08-11 10:23:02 ngocta2
	// string toChannel, string message
	public class ENetMQMessage
	{
		public string Channel { get; set; }
		public string Body { get; set; }
	}
}
