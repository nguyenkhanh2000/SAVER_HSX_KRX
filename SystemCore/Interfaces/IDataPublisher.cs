using System;
using System.Collections.Generic;
using System.Text;
using SystemCore.Entities;
using SystemCore.Interfaces;
using SystemCore.SharedKernel;

namespace SystemCore.Interfaces
{
	public interface IDataPublisher
	{
		// props
		long TotalMessageSent { get; }
		long TotalByteSent { get; }// tinh theo ASCII, "ôêốớ"=4
		DateTime LastSentTime { get; }
		//ETransportConfig TransportConfig { get; }
		EPub PubConfig { get; }
		string HBChannel { get; }

		// methods
		bool CreatePublisher();
		bool DestroyPublisher();
		long PublishMessage(string toChannel, string message);
	}
}
