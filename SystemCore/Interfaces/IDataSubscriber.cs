using System;
using System.Collections.Generic;
using System.Text;
using SystemCore.Entities;
using SystemCore.Interfaces;
using SystemCore.SharedKernel;

namespace SystemCore.Interfaces
{
	public delegate void OnMessageEventHandler(string channel, string message);
	public interface IDataSubscriber
	{
		// props
		long TotalMessageReceived { get; }
		long TotalByteReceived { get; } // tinh theo ASCII, "ôêốớ"=4
		DateTime LastReceivedTime { get; }
		//ETransportConfig TransportConfig { get; }
		EPub PubConfig { get; }
		ESub SubConfig { get; }
		OnMessageEventHandler OnMessage { get; set; }
		bool IsAlive { get; }
		bool IsPublishing { get; }

		// methods
		void SubscribeToChannel(string channel);
		bool UnsubscribeToChannel(string channel);

		/*
		Action<string, string> OnMessage { get; set; } // ServiceStack.Redis
		  
		subscription.OnMessage = (channel, msg) =>
		{
			try
			{       
			}
			catch (Exception ex)
			{
			}
		};
		*/

	}
}
