using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BaseSqlServerLib.Interfaces;
using CommonLib.Interfaces;
using Dapper;
using SystemCore.Entities;
using SystemCore.Temporaries;

namespace BaseSqlServerLib.Implementations
{

	/// <summary>
	/// 2019-11-19 13:31:19 ngocta2
	/// CSqlServer su dung Dapper de access db (MsSql Server)
	/// => ko su dung EF.core vi van de performance (EF.core cham hon Dapper)
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class CSqlServer<T> : ISqlServer<T> where T : class
	{
		// const
	

		// vars
		private readonly IS6GApp _app;
		private readonly string _connectionString;
		private SemaphoreSlim _semaphore = new SemaphoreSlim(1,1);
        // ==============================================================
      //  private readonly SqlConnection connection;
        /// <summary>
        /// 2019-11-18 08:48:20 ngocta2
        /// constructor
        /// </summary>
        /// <param name="app"><connection/param>
        /// <param name="String"></param>
        public CSqlServer(IS6GApp app, string connectionString)
		{
            this._app = app;              
			this._connectionString = connectionString;
        }

		/// <summary>
		/// 2019-11-20 16:02:36 ngocta2
		/// lay thong tin param (name+value) trong DynamicParameters , muc dich de debug
		/// https://stackoverflow.com/questions/10501319/is-there-anyway-to-iterate-through-a-dapper-dynamicparameters-object
		/// </summary>
		/// <param name="parameters"></param>
		/// <returns></returns>
		public string ParametersToString(DynamicParameters parameters)
		{
			var result = new StringBuilder();

			if (parameters != null)
			{
				var firstParam = true;
				var parametersLookup = (SqlMapper.IParameterLookup)parameters;
				foreach (var paramName in parameters.ParameterNames)
				{
					if (!firstParam)
					{
						result.Append(", "); //"\r\n, "
					}
					firstParam = false;

					result.Append('@');
					result.Append(paramName);
					result.Append(" = ");
					try
					{
						var value = parametersLookup[paramName];// parameters.Get<dynamic>(paramName);

						//((System.Collections.Generic.Dictionary<string, Dapper.DynamicParameters.ParamInfo>)parameters.parameters)["Name"].DbType

						result.Append((value != null) ? $"'{value.ToString()}'" : EGlobalConfig.__STRING_NULL);
					}
					catch
					{
						result.Append(EGlobalConfig.__STRING_UNKNOWN);
					}
				}

			}
			return result.ToString();
		}

		/// <summary>
		/// 2019-11-20 16:50:26 ngocta2
		/// lay thong tin param cua param list
		/// </summary>
		/// <param name="paramList"></param>
		/// <returns>moi param string tren 1 dong</returns>
		public string ParameterListToString(List<DynamicParameters> paramList)
		{
			var result = new StringBuilder();

			foreach (DynamicParameters parameters in paramList)
			{
				string paramInfo = ParametersToString(parameters);
				result.Append(paramInfo.Length > 0 ? EGlobalConfig.__STRING_RETURN_NEW_LINE + paramInfo : EGlobalConfig.__STRING_BLANK);
			}
				

			return result.ToString();
		}

        /// <summary>
        /// 2019-11-19 14:05:57 ngocta2
        /// su dung trong cac truong hop: insert, update, delete
        /// </summary>
        /// <param name="sql">
        /// insert into tbl_TEST(name) values('aaaaa')
        /// </param>
        /// <returns>1</returns>
        public async Task<int> ExecuteAsync(string sql)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} {sql}", true);

            int affectedRows = 0;
            try
            {
                using (SqlConnection connection = new SqlConnection(this._connectionString))
                //using (connection)
                {
                    await connection.OpenAsync();

                    // log before
                    //this._app.SqlLogger.LogSql(ec.Data);
                    // exec async
                    affectedRows = await connection.ExecuteAsync(sql);
                    // log after: khong lay duoc data return, output sau khi exec; caller phai tu log neu can
                    //this._app.SqlLogger.LogSql(this._app.Common.GetResultInfo(affectedRows));
                    await connection.CloseAsync();
                }
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
            }
            return affectedRows;
        }
		
        /// <summary>
        /// 2019-11-19 14:24:52 ngocta2
        /// su dung trong truong hop: select nhieu row => map luon vao T type
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> QueryAsync(string sql)
		{
			TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} {sql}", true);
			IEnumerable<T> list = new List<T>();
			try
			{
                using (SqlConnection connection = new SqlConnection(this._connectionString))
               // using (connection)
                {
					await connection.OpenAsync();

					// log before
					//this._app.SqlLogger.LogSql(ec.Data);
					// exec async
					list = await connection.QueryAsync<T>(sql);
					// log after: khong lay duoc data return, output sau khi exec; caller phai tu log neu can
					//this._app.SqlLogger.LogSql("list.ToList().Count=" + list.ToList().Count.ToString());
                    await connection.CloseAsync();
                }
			}
			catch (Exception ex)
			{
				// log error + buffer data
				this._app.ErrorLogger.LogErrorContext(ex, ec);
			}
			return list;
		}

		/// <summary>
		/// 2019-11-19 15:04:46 ngocta2
		/// su dung trong truong hop: select 1 row => map luon vao T type
		/// </summary>
		/// <param name="sql"></param>
		/// <returns></returns>
		public async Task<T> QuerySingleAsync(string sql)
		{
			TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} {sql}", true);
			T element = null;
			try
			{
                using (SqlConnection connection = new SqlConnection(this._connectionString))
                //using (connection)
                {
					await connection.OpenAsync();

					// log before
					//this._app.SqlLogger.LogSql(ec.Data);
					// exec async					
					// neu sql = "select id, name, date from tbl_TEST where id=1" nhung table ko co row voi id = 1 >>> error: Sequence contains no elements					
					element = await connection.QuerySingleOrDefaultAsync<T>(sql);   // khong bi nhay vao exception block, element = null
					//element = await connection.QuerySingleAsync<T>(sql);			// co bi nhay vao exception block, element = null

					// log after: khong lay duoc data return, output sau khi exec; caller phai tu log neu can
					//this._app.SqlLogger.LogSql(this._app.Common.SerializeObject(element));
                    await connection.CloseAsync();
                }
			}
			catch (Exception ex)
			{
				// log error + buffer data
				this._app.ErrorLogger.LogErrorContext(ex, ec);
			}
			return element;
		}

		/// <summary>
		/// 2019-11-19 15:47:36 ngocta2
		/// su dung trong truong hop: exec sp insert, update, delete => ko tra data (1 DataTable)
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		public async Task<EDalResult> ExecuteSpNoQueryAsync(string sql, DynamicParameters parameters)
		{
			TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} {sql} {ParametersToString(parameters)}", true);
			EDalResult result ;
			int affectedRows = 0;
			try
			{
                using (SqlConnection connection = new SqlConnection(this._connectionString))
               // using (connection)
                {
					await connection.OpenAsync();

					// log before
					//this._app.SqlLogger.LogSql(ec.Data);
					// exec async
					affectedRows = await connection.ExecuteAsync(sql, parameters, commandType: CommandType.StoredProcedure);
					// log after: khong lay duoc data return, output sau khi exec; caller phai tu log neu can	
					//this._app.SqlLogger.LogSql(this._app.Common.GetResultInfo(affectedRows));
                    await connection.CloseAsync();
                }
				result = new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EDalResult.__STRING_SUCCESS, Data = affectedRows };
			}
			catch (Exception ex)
			{
				// log error + buffer data
				this._app.ErrorLogger.LogErrorContext(ex, ec);
				result = new EDalResult() { Code = EDalResult.__CODE_ERROR, Message = ex.Message, Data = affectedRows };
			}
			return result;
		}

		/// <summary>
		/// 2019-11-20 14:09:10 ngocta2
		/// su dung trong truong hop: exec sp insert, update, delete => ko tra data (1 DataTable)
		/// => xu ly nhieu row 1 luc, vi truyen 1 list cac param
		/// --------------------------------
		/// cach xu ly cu la tao sql script co dang sau roi exec 1 lan
		/// exec prc_INSERT 'p1', 'p2', 'p3'
		/// exec prc_INSERT 'p1', 'p2', 'p3'
		/// co the tao sql rat dai, de insert ca tram row 1 luc roi exec 1 lan
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="paramList"></param>
		/// <returns></returns>
		public async Task<EDalResult> ExecuteSpNoQueryManyAsync(string sql, List<DynamicParameters> paramList)
		{
			TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} {sql} {ParameterListToString(paramList)}", true);
			EDalResult result;
			int affectedRows = 0;
			try
			{
                using (SqlConnection connection = new SqlConnection(this._connectionString))
               // using (connection)
                {
					await connection.OpenAsync();

					// log before
					//this._app.SqlLogger.LogSql(ec.Data);
					// exec async
					affectedRows = await connection.ExecuteAsync(sql, paramList, commandType: CommandType.StoredProcedure);
					// log after: khong lay duoc data return, output sau khi exec; caller phai tu log neu can					
					//this._app.SqlLogger.LogSql(this._app.Common.GetResultInfo(affectedRows));
                    await connection.CloseAsync();
                }
				result = new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EDalResult.__STRING_SUCCESS, Data = affectedRows };
			}
			catch (Exception ex)
			{
				// log error + buffer data
				this._app.ErrorLogger.LogErrorContext(ex, ec);
				result = new EDalResult() { Code = EDalResult.__CODE_ERROR, Message = ex.Message, Data = affectedRows };
			}
			return result;
		}

		/// <summary>
		/// 2019-11-19 16:06:43 ngocta2
		/// su dung trong truong hop: exec sp select => co tra data (1 DataTable)
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		public async Task<EDalResult> ExecuteSpQueryAsync(string sql, DynamicParameters parameters)
		{
			TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} {sql} {ParametersToString(parameters)}", true);

			EDalResult result;
			IEnumerable<T> list;
			try
			{
                using (SqlConnection connection = new SqlConnection(this._connectionString))
              //  using (connection)
                {
					await connection.OpenAsync();

					// log before
					//this._app.SqlLogger.LogSql(ec.Data);
					// exec async
					list = await connection.QueryAsync<T>(sql, parameters, commandType: CommandType.StoredProcedure);
					// log after: khong lay duoc data return, output sau khi exec; caller phai tu log neu can
					//this._app.SqlLogger.LogSql("list.ToList().Count=" + list.ToList().Count.ToString());
                    await connection.CloseAsync();
                }
				result = new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EDalResult.__STRING_SUCCESS, Data = list };
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
		/// 2019-11-19 16:44:58 ngocta2
		/// su dung trong truong hop: exec sp select => co tra data (2 DataTable tro len, tuong duong 1 DataSet trong ADO.net)
		/// https://stackoverflow.com/questions/5962117/is-there-a-way-to-call-a-stored-procedure-with-dapper
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		public async Task<EDalResult> ExecuteSpQueryMultipleAsync(string sql, DynamicParameters parameters)
		{
			TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} {sql} {ParametersToString(parameters)}", true);
			EDalResult result;
			SqlMapper.GridReader gridReader;
			List<List<dynamic>> dataSet = new List<List<dynamic>>();

			try
			{
                using (SqlConnection connection = new SqlConnection(this._connectionString))
               // using (connection)
                {
					await connection.OpenAsync();

					// log before
					//this._app.SqlLogger.LogSql(ec.Data);
					// exec async
					gridReader = await connection.QueryMultipleAsync(sql, parameters, commandType: CommandType.StoredProcedure);
					// log after: khong lay duoc data return, output sau khi exec; caller phai tu log neu can					

					// store
					while (!gridReader.IsConsumed)
					{
						List<dynamic> dataTable = gridReader.Read<dynamic>().ToList();
						dataSet.Add(dataTable);
					}

					//this._app.SqlLogger.LogSql("dataSet.Count=" + dataSet.Count.ToString());
                    await connection.CloseAsync();
                }
				// ra ngoai block using la reader bi close, phai gan cac dataTable vao dataSet ngay truoc khi ra ngoai using block
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
		/// 2021-02-26 10:25:55 ngocta2
		/// su dung trong truong hop: exec sp select => co tra data (2 DataTable tro len, tuong duong 1 DataSet trong ADO.net)
		/// https://dapper-tutorial.net/async
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		public async Task<EDalResult> ExecuteSpQueryMultiple2FuncAsync(Func<SqlMapper.GridReader, object> assignFunction, string sql, DynamicParameters parameters)
		{
			TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} {sql} {ParametersToString(parameters)}", true);
			EDalResult result;			
			object data;

			try
			{
              	using (SqlConnection connection = new SqlConnection(this._connectionString))
              //  using (connection)
                {
					await connection.OpenAsync();

					// log before
					//this._app.SqlLogger.LogSql(ec.Data);
					// exec async
					SqlMapper.GridReader gridReader = await connection.QueryMultipleAsync(sql, parameters, commandType: CommandType.StoredProcedure);
					// log after: khong lay duoc data return, output sau khi exec; caller phai tu log neu can					

					// function gan value vao object, phai thuc hien truoc khi ra khoi {} context nay
					data = assignFunction(gridReader);
                    await connection.CloseAsync();
                }
				// ra ngoai block using la reader bi close, phai gan cac dataTable vao dataSet ngay truoc khi ra ngoai using block
				result = new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EDalResult.__STRING_SUCCESS, Data = data};
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
		/// 2020-08-04 13:14:36 ngocta2
		/// chi lay sql script
		/// >>> exec bulk script (insert k rows)
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		public string GetScript(string sql, DynamicParameters parameters)
		{
			return $"{sql} {ParametersToString(parameters)}";
		}
	}

}
