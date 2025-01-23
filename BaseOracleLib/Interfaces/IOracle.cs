using Oracle.ManagedDataAccess.Client;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using SystemCore.Entities;

namespace BaseOracleLib.Interfaces
{
	public interface IOracle
	{
		string ParametersToString(OracleParameter[] parameters);
		Task<EDalResult> ExecuteSpNoQueryAsync(string sql, OracleParameter[] parameters);
		Task<EDalResult> ExecuteSpQueryAsync(string sql, OracleParameter[] parameters);
		string GetScript(string sql, OracleParameter[] parameters);
		string ParametersToStringWithoutName(OracleParameter[] parameters);
		string ToOracleDateTimeString(object sendingTime);
		Task<EDalResult> ExecuteAsync(string sql);
	}
}
