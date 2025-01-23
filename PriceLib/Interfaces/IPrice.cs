using MDDSCore.Messages;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using SystemCore.Entities;

namespace PriceLib.Interfaces
{
	public interface IPrice
	{
		Task<EDalResult> MDDS_MSSQL_UpdateSecurityDefinition(List<ESecurityDefinition>  eSecurityDefinitions);
		Task<EDalResult> MDDS_ORACLE_UpdateSecurityDefinition(List<ESecurityDefinition> eSecurityDefinitions);
	}
}
