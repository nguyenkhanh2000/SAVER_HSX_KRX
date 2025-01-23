using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Interfaces
{
	//2020-08-07 09:45:44 ngocta2
	public interface IDataConsumer: IDisposable
	{
		void CheckSubStatus();
		void CheckSubData();
		void ReInitSubscriber();
		bool InitSubscriber();
		void OnReceived(string channel, string message);
    }
}
