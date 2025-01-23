using System;
using System.Collections.Generic;
using System.Text;

namespace BaseSqlServerLib.Interfaces
{
	public interface IDemo
	{
		int ConnectSqlServerUsingDomainUser();
		int ConnectSqlServerUsingSqlUser();
		List<T> GetData<T>(int id);
	}
}
