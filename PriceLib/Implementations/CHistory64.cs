using BaseSqlServerLib.Implementations;
using BaseSqlServerLib.Interfaces;
using CommonLib.Interfaces;
using Dapper;
using PriceLib.Interfaces;
using StockCore.History;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using SystemCore.Entities;
using SystemCore.Temporaries;

namespace PriceLib.Implementations
{
    /// <summary>
    /// 2021-03-01 13:31:02 ngocta2
    /// class lay du lieu lich su trong STCADAPTER 10.26.248.64 trong hien tai
    /// nhung trong tuong lai chac chan se khong lay data tai STCADAPTER 10.26.248.64 nua vi MSSQL sap bi xoa
    /// co the la sau nay se lay data hist tu Oracle, nhung class do cung se van phai la implement interface IHistory (vd class CHistoryOracle: CBaseHistory, IHistory) 
    /// </summary>
    public class CHistory64 : CBaseHistory, IHistory
    {

		public CHistory64(IS6GApp s6GApp, EHistoryConfig eHistoryConfig) : base( s6GApp,  eHistoryConfig)
        {

        }

		/// <summary>
		/// 2021-02-25 17:24:10 ngocta2
		/// lay du lieu history index trong db stcadapter 64
		/// </summary>
		/// <param name="hs"></param>
		/// <returns></returns>
	/*	public async Task<EDalResult> GetHistIndex(EHistSearch hs)
        {
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} histSearch={this._cS6GApp.Common.SerializeObject(hs)}", true);

            try
            {
                // 0. init
                ISqlServer<EHistIndex> sqlServer = new CSqlServer<EHistIndex>(this._cS6GApp, this._eHistoryConfig.ConnectionMssqlDbStcadapter);
                string spName = this._eHistoryConfig.SpStcadapter64HistGetIndex;
                EDalResult result;

                // 1. input            
                DynamicParameters dynamicParameters = new DynamicParameters();

                dynamicParameters.Add($"@{__CENTER}",       hs.Center,       DbType.Int32,      ParameterDirection.Input);
                dynamicParameters.Add($"@{__TIME}",         hs.Time,         DbType.Int32,      ParameterDirection.Input);
                dynamicParameters.Add($"@{__BEGINDATE}",    hs.BeginDate,    DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__ENDDATE}",      hs.EndDate,      DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__SELECTEDPAGE}", hs.SelectedPage, DbType.Int32,      ParameterDirection.Input);
                dynamicParameters.Add($"@{__PAGESIZE}",     hs.PageSize,     DbType.Int32,      ParameterDirection.Input);

                // 2. main			
                result = await sqlServer.ExecuteSpQueryMultiple2FuncAsync(GetHistIndexAssign, spName, dynamicParameters);

                // log sql afer (output)
                this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

                // return (neu sp ko tra error code,msg thi tu gan default)
                return new EDalResult() { Code = result.Code, Message = result.Message, Data = result.Data };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._cS6GApp.ErrorLogger.LogErrorContext(ex, ec);
                // error => return null
                return new EDalResult() { Code = EGlobalConfig.__CODE_ERROR_IN_LAYER_DAL, Message = ex.Message, Data = null };
            }
		}
*/
        /// <summary>
		/// 2021-01-03 15:24:10 hungtq2
		/// lay du lieu history stock price trong db stcadapter 64
		/// </summary>
		/// <param name="hs"></param>
		/// <returns></returns>
	/*	public async Task<EDalResult> GetHistQuotePrice(EHistSearch hs)
        {
            // log input
            *//*TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} histSearch={this._cS6GApp.Common.SerializeObject(hs)}", true);

            try
            {
                // 0. init
                ISqlServer<EHistPrice> sqlServer = new CSqlServer<EHistPrice>(this._cS6GApp, this._eHistoryConfig.ConnectionMssqlDbStockInfoStore);
                string spName = this._eHistoryConfig.SpStcadapter64HistGetStockPrice;
                EDalResult result;

                // 1. input            
                DynamicParameters dynamicParameters = new DynamicParameters();

                dynamicParameters.Add($"@{__CENTER}",       hs.Center,       DbType.Int32, ParameterDirection.Input);
                dynamicParameters.Add($"@{__CODE}",         hs.Code,         DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__TYPE}",         hs.Type,         DbType.Int32, ParameterDirection.Input);
                dynamicParameters.Add($"@{__DATE}",         hs.Date,         DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__BEGINDATE}",    hs.BeginDate,    DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__ENDDATE}",      hs.EndDate,      DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__SELECTEDPAGE}", hs.SelectedPage, DbType.Int32, ParameterDirection.Input);
                dynamicParameters.Add($"@{__PAGESIZE}",     hs.PageSize,     DbType.Int32, ParameterDirection.Input);  

                // 2. main			
                result = await sqlServer.ExecuteSpQueryMultiple2FuncAsync(GetHistQuotePriceAssign, spName, dynamicParameters);

                // log sql afer (output)
                this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

                // return (neu sp ko tra error code,msg thi tu gan default)
                return new EDalResult() { Code = result.Code, Message = result.Message, Data = result.Data };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._cS6GApp.ErrorLogger.LogErrorContext(ex, ec);
                // error => return null
                return new EDalResult() { Code = EGlobalConfig.__CODE_ERROR_IN_LAYER_DAL, Message = ex.Message, Data = null };
            }*//*
        }
*/
        /// <summary>
		/// 2021-01-03 15:24:10 hungtq2
		/// lay du lieu history stock order trong db stcadapter 64
		/// </summary>
		/// <param name="hs"></param>
		/// <returns></returns>
	/*	public async Task<EDalResult> GetHistQuoteOrder(EHistSearch hs)
        {
            // log input
    *//*        TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} histSearch={this._cS6GApp.Common.SerializeObject(hs)}", true);

            try
            {
                // 0. init
                ISqlServer<EHistOrder> sqlServer = new CSqlServer<EHistOrder>(this._cS6GApp, this._eHistoryConfig.ConnectionMssqlDbStockInfoStore);
                string spName = this._eHistoryConfig.SpStcadapter64HistGetStockOrder;
                EDalResult result;

                // 1. input            
                DynamicParameters dynamicParameters = new DynamicParameters();

                dynamicParameters.Add($"@{__CENTER}",       hs.Center,       DbType.Int32, ParameterDirection.Input);
                dynamicParameters.Add($"@{__CODE}",         hs.Code,         DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__TYPE}",         hs.Type,         DbType.Int32, ParameterDirection.Input);
                dynamicParameters.Add($"@{__DATE}",         hs.Date,         DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__BEGINDATE}",    hs.BeginDate,    DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__ENDDATE}",      hs.EndDate,      DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__SELECTEDPAGE}", hs.SelectedPage, DbType.Int32, ParameterDirection.Input);
                dynamicParameters.Add($"@{__PAGESIZE}",     hs.PageSize,     DbType.Int32, ParameterDirection.Input); 

                // 2. main			
                result = await sqlServer.ExecuteSpQueryMultiple2FuncAsync(GetHistQuoteOrderAssign, spName, dynamicParameters);

                // log sql afer (output)
                this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

                // return (neu sp ko tra error code,msg thi tu gan default)
                return new EDalResult() { Code = result.Code, Message = result.Message, Data = result.Data };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._cS6GApp.ErrorLogger.LogErrorContext(ex, ec);
                // error => return null
                return new EDalResult() { Code = EGlobalConfig.__CODE_ERROR_IN_LAYER_DAL, Message = ex.Message, Data = null };
            }*//*
        }
*/
        /// <summary>
        /// 2021-01-03 15:24:10 hungtq2
        /// lay du lieu history stock Foregin_NM trong db stcadapter 64
        /// </summary>
        /// <param name="hs"></param>
        /// <returns></returns>
   /*     public async Task<EDalResult> GetHistQuoteForeginNM(EHistSearch hs)
        {
            // log input
        *//*    TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} histSearch={this._cS6GApp.Common.SerializeObject(hs)}", true);

            try
            {
                // 0. init
                ISqlServer<EHistForeginNM> sqlServer = new CSqlServer<EHistForeginNM>(this._cS6GApp, this._eHistoryConfig.ConnectionMssqlDbStockInfoStore);
                string spName = this._eHistoryConfig.SpStcadapter64HistGetStockForeginNM;
                EDalResult result;

                // 1. input            
                DynamicParameters dynamicParameters = new DynamicParameters();

                dynamicParameters.Add($"@{__CENTER}",       hs.Center,       DbType.Int32, ParameterDirection.Input);
                dynamicParameters.Add($"@{__TYPE}",         hs.Type,         DbType.Int32, ParameterDirection.Input);
                dynamicParameters.Add($"@{__CODE}",         hs.Code,         DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__DATE}",         hs.Date,         DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__BEGINDATE}",    hs.BeginDate,    DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__ENDDATE}",      hs.EndDate,      DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__SELECTEDPAGE}", hs.SelectedPage, DbType.Int32, ParameterDirection.Input);
                dynamicParameters.Add($"@{__PAGESIZE}",     hs.PageSize,     DbType.Int32, ParameterDirection.Input);

                // 2. main			
                result = await sqlServer.ExecuteSpQueryMultiple2FuncAsync(GetHistQuoteForeginNMAssign, spName, dynamicParameters);

                // log sql afer (output)
                this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

                // return (neu sp ko tra error code,msg thi tu gan default)
                return new EDalResult() { Code = result.Code, Message = result.Message, Data = result.Data };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._cS6GApp.ErrorLogger.LogErrorContext(ex, ec);
                // error => return null
                return new EDalResult() { Code = EGlobalConfig.__CODE_ERROR_IN_LAYER_DAL, Message = ex.Message, Data = null };
            }*//*
        }
*/
        /// <summary>
        /// 2021-01-03 15:24:10 hungtq2
        /// lay du lieu history stock Foregin_PT trong db stcadapter 64
        /// </summary>
        /// <param name="hs"></param>
        /// <returns></returns>
     /*   public async Task<EDalResult> GetHistQuoteForeginPT(EHistSearch hs)
        {
            // log input
           *//* TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} histSearch={this._cS6GApp.Common.SerializeObject(hs)}", true);

            try
            {
                // 0. init
                ISqlServer<EHistForeginPT> sqlServer = new CSqlServer<EHistForeginPT>(this._cS6GApp, this._eHistoryConfig.ConnectionMssqlDbStockInfoStore);
                string spName = this._eHistoryConfig.SpStcadapter64HistGetStockForeginPT;
                EDalResult result;

                // 1. input            
                DynamicParameters dynamicParameters = new DynamicParameters();

                dynamicParameters.Add($"@{__CENTER}",       hs.Center,       DbType.Int32, ParameterDirection.Input);
                dynamicParameters.Add($"@{__TYPE}",         hs.Type,         DbType.Int32, ParameterDirection.Input);
                dynamicParameters.Add($"@{__CODE}",         hs.Code,         DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__DATE}",         hs.Date,         DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__BEGINDATE}",    hs.BeginDate,    DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__ENDDATE}",      hs.EndDate,      DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__SELECTEDPAGE}", hs.SelectedPage, DbType.Int32, ParameterDirection.Input);
                dynamicParameters.Add($"@{__PAGESIZE}",     hs.PageSize,     DbType.Int32, ParameterDirection.Input);

                // 2. main			
                result = await sqlServer.ExecuteSpQueryMultiple2FuncAsync(GetHistQuoteForeginPTAssign, spName, dynamicParameters);

                // log sql afer (output)
                this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

                // return (neu sp ko tra error code,msg thi tu gan default)
                return new EDalResult() { Code = result.Code, Message = result.Message, Data = result.Data };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._cS6GApp.ErrorLogger.LogErrorContext(ex, ec);
                // error => return null
                return new EDalResult() { Code = EGlobalConfig.__CODE_ERROR_IN_LAYER_DAL, Message = ex.Message, Data = null };
            }*//*
        }
*/
        /// <summary>
        /// 2021-02-26 14:46:48 ngocta2
        /// sub function cho GetHistIndex
        /// </summary>
        /// <param name="grid"></param>
        /// <returns></returns>
        public object GetHistIndexAssign(SqlMapper.GridReader grid)
        {            
            EHistIndex eHistIndex = new EHistIndex();
            eHistIndex.Header     = grid.ReadFirst<EHistBaseHeader>();
            eHistIndex.Body       = grid.Read<EHistItemIndex>().AsList();
            return eHistIndex;
        }

        /// <summary>
        /// 2021-01-03 15:24:10 hungtq2
        /// sub function cho GetHistStockPrice
        /// </summary>
        /// <param name="grid"></param>
        /// <returns></returns>
        public object GetHistQuotePriceAssign(SqlMapper.GridReader grid)
        {
            EHistPrice eHistPrice = new EHistPrice();
            eHistPrice.Header     = grid.ReadFirst<EHistBaseHeader>();
            eHistPrice.Body       = grid.Read<EHistItemPrice>().AsList();
            return eHistPrice;
        }

        /// <summary>
        /// 2021-01-03 15:24:10 hungtq2
        /// sub function cho GetHistStockOrder
        /// </summary>
        /// <param name="grid"></param>
        /// <returns></returns>
        public object GetHistQuoteOrderAssign(SqlMapper.GridReader grid)
        {
            EHistOrder eHistOrder = new EHistOrder();
            eHistOrder.Header     = grid.ReadFirst<EHistBaseHeader>();
            eHistOrder.Body       = grid.Read<EHistItemOrder>().AsList();
            return eHistOrder;
        }

        /// <summary>
        /// 2021-01-03 15:24:10 hungtq2
        /// sub function cho GetHistForegin_NM
        /// </summary>
        /// <param name="grid"></param>
        /// <returns></returns>
        public object GetHistQuoteForeginNMAssign(SqlMapper.GridReader grid)
        {
            EHistForeginNM eHistForeginNM = new EHistForeginNM();
            eHistForeginNM.Header         = grid.ReadFirst<EHistBaseHeader>();
            eHistForeginNM.Body           = grid.Read<EHistItemForeginNM>().AsList();
            return eHistForeginNM;
        }

        /// <summary>
        /// 2021-01-03 15:24:10 hungtq2
        /// sub function cho GetHistForegin_PT
        /// </summary>
        /// <param name="grid"></param>
        /// <returns></returns>
        public object GetHistQuoteForeginPTAssign(SqlMapper.GridReader grid)
        {
            EHistForeginPT eHistForeginPT = new EHistForeginPT();
            eHistForeginPT.Header = grid.ReadFirst<EHistBaseHeader>();
            eHistForeginPT.Body = grid.Read<EHistItemForeginPT>().AsList();
            return eHistForeginPT;
        }
    }
}
