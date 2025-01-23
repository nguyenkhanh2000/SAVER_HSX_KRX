using System;
using System.Collections.Generic;
using System.Text;

namespace BaseSaverLib.Interfaces
{
	public interface ISaver
	{
		bool InitApp();
		bool ReceiveMessageFromMessageQueue(string messageBlock);
	}
}
