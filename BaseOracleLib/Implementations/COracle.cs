using BaseOracleLib.Interfaces;
using BaseOracleLib.Library;
using CommonLib.Interfaces;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using SystemCore.Entities;
using SystemCore.Temporaries;

namespace BaseOracleLib.Implementations
{
	public class COracle : IOracle
	{
		// const
		public const string CHAR_CRLF = "\r\n";
		public const string CHAR_TAB = "\t";
      
        // vars
        private readonly IS6GApp _app;
		private readonly string _connectionString;
        //private string connection;
        // ==============================================================

        /// <summary>
        /// 2019-11-18 08:48:20 ngocta2
        /// constructor
        /// </summary>
        /// <param name="app"></param>
        /// <param name="connectionString"></param>
        public COracle(IS6GApp app, string connectionString)
		{			
                this._app = app;
                this._connectionString = connectionString;
                     
        }

		/// <summary>
		/// 2019-11-25 13:48:27 ngocta2
		/// ke thua tu AuthenLib
		/// </summary>
		/// <param name="paramArray"></param>
		/// <returns></returns>
		public string ParametersToString(OracleParameter[] paramArray)
		{
			if (paramArray == null || paramArray.Length == 0)
				return EGlobalConfig.__STRING_BLANK;

			string infoString = "";//  CHAR_CRLF;// 2020-08-03 13:22:22 ngocta2 bo ky tu xuong dong
			for (int i = 0; i < paramArray.Length; i++)
				infoString += /*CHAR_TAB +*/ paramArray[i].ParameterName + "='" + paramArray[i].Value + "',";// + CHAR_CRLF; // 2020-08-03 13:22:22 ngocta2 bo ky tu xuong dong

			return infoString;
		}

		/// <summary>
		/// 2019-11-25 16:50:38 ngocta2
		/// co tra data, 
		/// thich hop cho SELECT
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		public async Task<EDalResult> ExecuteSpQueryAsync(string sql, OracleParameter[] parameters)
		{
			TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} {sql} {ParametersToString(parameters)}", true);
			EDalResult result;
			DataSet dataSet;

			try
			{
				using (OracleConnection connection = new OracleConnection(this._connectionString))
				{

					await connection.OpenAsync();

					// log before
					//this._app.SqlLogger.LogSql(ec.Data);
					// exec sync
					dataSet = OracleHelper.ExecuteDataset(connection, CommandType.StoredProcedure, sql, parameters);
					// log after
					//this._app.SqlLogger.LogSql(this._app.Common.GetResultInfo(dataSet));

                    await connection.CloseAsync();
                }
				// return
				result = new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EDalResult.__STRING_SUCCESS, Data = dataSet };
			}
			catch (Exception ex)
			{
				// log error + buffer data
				this._app.ErrorLogger.LogErrorContext(ex, ec);
				result = new EDalResult() { Code = EDalResult.__CODE_ERROR, Message = ex.Message, Data = null };
			}
			return result;
		}


		/// <summary>
		/// 2019-11-25 16:50:38 ngocta2
		/// khong tra du lieu , khong SELECT
		/// thich hop cho : INSERT, UPDATE, DELETE
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="parameters"></param>
		/// <returns>
		/// System.Data.CommandType.StoredProcedure => -1
		/// </returns>
		public async Task<EDalResult> ExecuteSpNoQueryAsync(string sql, OracleParameter[] parameters)
		{
			TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} {sql} {ParametersToString(parameters)}", true);
			EDalResult result;
			int affectedRowCount = 0;

			try
			{
				using (OracleConnection connection = new OracleConnection(this._connectionString))
				{

					await connection.OpenAsync();

					// log before
					//this._app.SqlLogger.LogSql(ec.Data);
					// exec sync : return rows affected by the command
					affectedRowCount = OracleHelper.ExecuteNonQuery(connection, CommandType.StoredProcedure, sql, parameters);
					// log after
					//this._app.SqlLogger.LogSql(this._app.Common.GetResultInfo(affectedRowCount));
                    await connection.CloseAsync();
                }
				// return
				result = new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EDalResult.__STRING_SUCCESS, Data = affectedRowCount };
			}
			catch (Exception ex)
			{
				// log error + buffer data
				this._app.ErrorLogger.LogErrorContext(ex, ec);
				result = new EDalResult() { Code = EDalResult.__CODE_ERROR, Message = ex.Message, Data = null };
			}
			return result;
		}

		/// <summary>
		/// 2020-08-05 09:32:50 ngocta2
		/// chi lay sql script
		/// >>> exec bulk script (insert k rows)
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		public string GetScript(string sql, OracleParameter[] parameters)
		{
			return $"{sql} ({ParametersToStringWithoutName(parameters)});";
		}

		/// <summary>
		/// 2020-08-05 10:56:47 ngocta2
		/// tao script de update bulk
		/// </summary>
		/// <param name="paramArray"></param>
		/// <returns></returns>
		public string ParametersToStringWithoutName(OracleParameter[] paramArray)
		{
			if (paramArray == null || paramArray.Length == 0)
				return EGlobalConfig.__STRING_BLANK;

			string infoString = "";//  CHAR_CRLF;// 2020-08-03 13:22:22 ngocta2 bo ky tu xuong dong
			for (int i = 0; i < paramArray.Length; i++)
				infoString += "'" + paramArray[i].Value + "',";// + CHAR_CRLF; // 2020-08-03 13:22:22 ngocta2 bo ky tu xuong dong

			infoString = infoString.Substring(0, infoString.Length - 1-6);

			// replace ,'','') thanh ,pReturnCode,pReturnMess);
			//infoString = infoString.Replace(",'','')", ",pReturnCode,pReturnMess);");
			infoString += ",pReturnCode,pReturnMess";

			return infoString;
		}

		/// <summary>
		/// 2020-08-05 13:28:47 ngocta2
		/// </summary>
		/// <param name="sendingTime">datetime object</param>
		/// <returns>'2003-12-15 22:13:18'</returns>
		public string ToOracleDateTimeString(object sendingTime)
		{
			return Convert.ToDateTime(sendingTime).ToString(EGlobalConfig.DATETIME_ORACLE);
		}

		/// <summary>
		/// 2020-08-05 14:19:53 ngocta2
		/// insert bulk
		/// </summary>
		/// <param name="sql"></param>
		/// <returns></returns>
		public async Task<EDalResult> ExecuteAsync(string sql)
		{
			TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} {sql}", true);
			EDalResult result;
			int affectedRowCount = 0;

			try
			{
				using (OracleConnection connection = new OracleConnection(this._connectionString))
				{
                    await connection.OpenAsync();
                    
					
					// log before
					//this._app.SqlLogger.LogSql(ec.Data);
					// exec sync : return rows affected by the command
					affectedRowCount = OracleHelper.ExecuteNonQuery(connection, CommandType.Text, sql);
					// log after
					//this._app.SqlLogger.LogSql(this._app.Common.GetResultInfo(affectedRowCount));
                    await connection.CloseAsync();

                }
				// return
				result = new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EDalResult.__STRING_SUCCESS, Data = affectedRowCount };
			}
			catch (Exception ex)
			{
				// log error + buffer data
				this._app.ErrorLogger.LogErrorContext(ex, ec);
				result = new EDalResult() { Code = EDalResult.__CODE_ERROR, Message = ex.Message, Data = null };
			}
			return result;
		}
	}
}
