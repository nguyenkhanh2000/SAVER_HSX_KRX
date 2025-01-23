using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Interfaces
{
	//2020-08-11 10:41:13 ngocta2
	public interface IDataProducer
	{
		bool InitPublisher();
		bool PublishMessage(string toChannel, string message);
	}
}
