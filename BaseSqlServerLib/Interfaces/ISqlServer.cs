using Dapper;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using SystemCore.Entities;

namespace BaseSqlServerLib.Interfaces
{
	public interface ISqlServer<T>
	{
		string ParametersToString(DynamicParameters parameters);
		string ParameterListToString(List<DynamicParameters> paramList);
		Task<int>  ExecuteAsync(string sql);
		Task<IEnumerable<T>> QueryAsync(string sql);
		Task<T> QuerySingleAsync(string sql);
		Task<EDalResult> ExecuteSpNoQueryAsync(string sql, DynamicParameters parameters);
		Task<EDalResult> ExecuteSpNoQueryManyAsync(string sql, List<DynamicParameters> paramList);
		Task<EDalResult> ExecuteSpQueryAsync(string sql, DynamicParameters parameters);
		Task<EDalResult> ExecuteSpQueryMultipleAsync(string sql, DynamicParameters parameters);
		Task<EDalResult> ExecuteSpQueryMultiple2FuncAsync(Func<SqlMapper.GridReader, object> assignFunction, string sql, DynamicParameters parameters);
		string GetScript(string sql, DynamicParameters parameters);

    }
}
