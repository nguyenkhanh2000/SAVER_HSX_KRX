using System;
using System.Collections.Generic;
using System.Text;

namespace BaseSaverLib.Interfaces
{
	public interface ISaver
	{
		bool InitApp();
		Task ReceiveMessageFromMessageQueue(string messageBlock);
	}
}
