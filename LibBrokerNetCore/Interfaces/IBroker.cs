using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace BaseBrokerLib.Interfaces
{
	public interface IBroker
	{
		public delegate void OnMessageEventHandler(string messageBlock);
				
		public OnMessageEventHandler OnMessage { get; set; }

		// props
		EBrokerConfig BrokerConfig { get; }
        IModel Channel {  get; }    
        // methods
        bool InitBroker();
		bool SendMessageToQueue(string messageBlock);
		bool SetupOnReceivedEventHandler();
        bool SendMessageToQueueIndex(string messageBlock);
        bool SendMessageToQueueET(string messageBlock);
        bool SendMessageToQueueLoLe_G4(string messageBlock);
        bool SendMessageToQueueBuyIn_G7(string messageBlock);
        bool SendMessageToQueueSellIn_G8(string messageBlock);
        bool SendMessageToQueuePT_T1(string messageBlock);
        bool SendMessageToQueuePT_T2(string messageBlock);
        bool SendMessageToQueuePT_T3(string messageBlock);
        bool SendMessageToQueuePT_T4(string messageBlock);
        bool SendMessageToQueuePT_T6(string messageBlock);
        bool SendMessageToQueuePT_T7(string messageBlock);
        bool SendMessageToQueueIndexHNX(string messageBlock);
    }
}
