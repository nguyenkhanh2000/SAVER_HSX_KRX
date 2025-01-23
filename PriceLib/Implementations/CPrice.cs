using BaseOracleLib.Implementations;
using BaseOracleLib.Interfaces;
using BaseSqlServerLib.Implementations;
using BaseSqlServerLib.Interfaces;
using CommonLib.Implementations;
using CommonLib.Interfaces;
using Dapper;
using MDDSCore.Messages;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;
using PriceLib.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using SystemCore.Entities;
using SystemCore.Temporaries;

namespace PriceLib.Implementations
{
	public class CPrice : IPrice
	{
		// interface
		private readonly IS6GApp _cS6GApp;
		private readonly EPriceConfig _ePriceConfig;
		/// <summary>
		/// constructor
		/// </summary>
		/// <param name="s6GApp"></param>
		public CPrice(IS6GApp s6GApp, EPriceConfig ePriceConfig)
		{
			this._cS6GApp = s6GApp;
			this._ePriceConfig = ePriceConfig;
		}


        /// <summary>
        /// 2020-07-23 10:46:30 ngocta2
        /// update data vao table SecurityDefinition trong MSSQL
        /// </summary>
        /// <param name="eSecurityDefinitions"></param>
        /// <returns></returns>
		public async Task<EDalResult> MDDS_MSSQL_UpdateSecurityDefinition(List<ESecurityDefinition> eSecurityDefinitions)
		{
            // log input
            TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eSecurityDefinitions={this._cS6GApp.Common.SerializeObject(eSecurityDefinitions)}", true);

            try
            {
				// 0. init
				ISqlServer<ESecurityDefinition> sqlServer = new CSqlServer<ESecurityDefinition>(this._cS6GApp, this._ePriceConfig.ConnectionMssql);
				List<DynamicParameters> paramList = new List<DynamicParameters>();
				string spName = this._ePriceConfig.SpMddsMssqlUpdateSecurityDefinition;
				EDalResult result;

				// 1. input            
				foreach(ESecurityDefinition eSD in eSecurityDefinitions)
				{
					DynamicParameters dynamicParameters = new DynamicParameters();
					dynamicParameters.Add("@aBeginString",                      eSD.BeginString,                      DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aBodyLength",                       eSD.BodyLength,                       DbType.Int64, ParameterDirection.Input);
					dynamicParameters.Add("@aMsgType",                          eSD.MsgType,                          DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aSenderCompID",                     eSD.SenderCompID,                     DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aTargetCompID",                     eSD.TargetCompID,                     DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aMsgSeqNum",                        eSD.MsgSeqNum,                        DbType.Int64, ParameterDirection.Input);
					dynamicParameters.Add("@aSendingTime",                      eSD.SendingTime,                      DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aMarketID",                         eSD.MarketID,                         DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aBoardID",                          eSD.BoardID,                          DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aTotNumReports",                    eSD.TotNumReports,                    DbType.Int64, ParameterDirection.Input);
					dynamicParameters.Add("@aSecurityExchange",                 eSD.SecurityExchange,                 DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aSymbol",                           eSD.Symbol,                           DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aTickerCode",                       eSD.TickerCode,                       DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aSymbolShortCode",                  eSD.SymbolShortCode,                  DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aSymbolName",                       eSD.SymbolName,                       DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aSymbolEnglishName",                eSD.SymbolEnglishName,                DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aProductID",                        eSD.ProductID,                        DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aProductGrpID",                     eSD.ProductGrpID,                     DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aSecurityGroupID",                  eSD.SecurityGroupID,                  DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aPutOrCall",                        eSD.PutOrCall,                        DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aExerciseStyle",                    eSD.ExerciseStyle,                    DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aMaturityMonthYear",                eSD.MaturityMonthYear,                DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aMaturityDate",                     eSD.MaturityDate,                     DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aIssuer",                           eSD.Issuer,                           DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aIssueDate",                        eSD.IssueDate,                        DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aContractMultiplier",               eSD.ContractMultiplier,               DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aCouponRate",                       eSD.CouponRate,                       DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aCurrency",                         eSD.Currency,                         DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aListedShares",                     eSD.ListedShares,                     DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aHighLimitPrice",                   eSD.HighLimitPrice,                   DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aLowLimitPrice",                    eSD.LowLimitPrice,                    DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aStrikePrice",                      eSD.StrikePrice,                      DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aSecurityStatus",                   eSD.SecurityStatus,                   DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aContractSize",                     eSD.ContractSize,                     DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aSettlMethod",                      eSD.SettlMethod,                      DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aYield",                            eSD.Yield,                            DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aReferencePrice",                   eSD.ReferencePrice,                   DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aEvaluationPrice",                  eSD.EvaluationPrice,                  DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aHgstOrderPrice",                   eSD.HgstOrderPrice,                   DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aLwstOrderPrice",                   eSD.LwstOrderPrice,                   DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aPrevClosePx",                      eSD.PrevClosePx,                      DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aSymbolCloseInfoPxType",            eSD.SymbolCloseInfoPxType,            DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aFirstTradingDate",                 eSD.FirstTradingDate,                 DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aFinalTradeDate",                   eSD.FinalTradeDate,                   DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aFinalSettleDate",                  eSD.FinalSettleDate,                  DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aListingDate",                      eSD.ListingDate,                      DbType.AnsiString, ParameterDirection.Input);
					//dynamicParameters.Add("@aOpenInterestQty",                  eSD.OpenInterestQty,                  DbType.AnsiString, ParameterDirection.Input);
					//dynamicParameters.Add("@aSettlementPrice",                  eSD.SettlementPrice,                  DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aREtriggeringconditioncode",		eSD.RandomEndTriggeringConditionCode, DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aExClassType",                      eSD.ExClassType,                      DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aVWAP",                             eSD.VWAP,                             DbType.Decimal, ParameterDirection.Input);
					dynamicParameters.Add("@aSymbolAdminStatusCode",            eSD.SymbolAdminStatusCode,            DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@@aSymbolTradingMethodSC",			eSD.SymbolTradingMethodStatusCode,    DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aSymbolTradingSantionSC",			eSD.SymbolTradingSantionStatusCode,   DbType.AnsiString, ParameterDirection.Input);
					dynamicParameters.Add("@aCheckSum",                         eSD.CheckSum,                         DbType.AnsiString, ParameterDirection.Input);
					paramList.Add(dynamicParameters);
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryManyAsync(spName, paramList);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

                // return (neu sp ko tra error code,msg thi tu gan default)
                return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EDalResult.__STRING_SUCCESS, Data = result.Data };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._cS6GApp.ErrorLogger.LogErrorContext(ex, ec);
                // error => return null
                return new EDalResult() { Code = EGlobalConfig.__CODE_ERROR_IN_LAYER_DAL, Message = ex.Message, Data = null };
            }
        }


		/// <summary>
		/// 2020-07-23 14:53:19 ngocta2
		/// update data vao table SecurityDefinition trong Oracle
		/// </summary>
		/// <param name="eSecurityDefinitions"></param>
		/// <returns></returns>
		public async Task<EDalResult> MDDS_ORACLE_UpdateSecurityDefinition(List<ESecurityDefinition> eSecurityDefinitions)
		{
            // log input
            TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eSecurityDefinitions={_cS6GApp.Common.SerializeObject(eSecurityDefinitions)}", true);

            try
            {
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				List<DynamicParameters> paramList = new List<DynamicParameters>();
				string spName = this._ePriceConfig.SpMddsOracleUpdateSecurityDefinition;
				EDalResult result = null;

				// 1. input            
				foreach(ESecurityDefinition eSD in eSecurityDefinitions)
				{
					OracleParameter[] paramArrary = new OracleParameter[]
					{
						 new OracleParameter("paBeginString",               OracleDbType.Varchar2, 8, eSD.BeginString, ParameterDirection.Input)
						,new OracleParameter("paBodyLength",                OracleDbType.Varchar2, 8, eSD.BodyLength, ParameterDirection.Input)
						,new OracleParameter("paMsgType",                   OracleDbType.Varchar2, 8, eSD.MsgType, ParameterDirection.Input)
						,new OracleParameter("paSenderCompID",              OracleDbType.Varchar2, 8, eSD.SenderCompID, ParameterDirection.Input)
						,new OracleParameter("paTargetCompID",              OracleDbType.Varchar2, 8, eSD.TargetCompID, ParameterDirection.Input)
						,new OracleParameter("paMsgSeqNum",                 OracleDbType.Varchar2, 8, eSD.MsgSeqNum, ParameterDirection.Input)
						,new OracleParameter("paSendingTime",               OracleDbType.Varchar2, 8, eSD.SendingTime, ParameterDirection.Input)
						,new OracleParameter("paMarketID",                  OracleDbType.Varchar2, 8, eSD.MarketID, ParameterDirection.Input)
						,new OracleParameter("paBoardID",                   OracleDbType.Varchar2, 8, eSD.BoardID, ParameterDirection.Input)
						,new OracleParameter("paTotNumReports",             OracleDbType.Varchar2, 8, eSD.TotNumReports, ParameterDirection.Input)
						,new OracleParameter("paSecurityExchange",          OracleDbType.Varchar2, 8, eSD.SecurityExchange, ParameterDirection.Input)
						,new OracleParameter("paSymbol",                    OracleDbType.Varchar2, 8, eSD.Symbol, ParameterDirection.Input)
						,new OracleParameter("paTickerCode",                OracleDbType.Varchar2, 8, eSD.TickerCode, ParameterDirection.Input)
						,new OracleParameter("paSymbolShortCode",           OracleDbType.Varchar2, 8, eSD.SymbolShortCode, ParameterDirection.Input)
						,new OracleParameter("paSymbolName",                OracleDbType.Varchar2, 8, eSD.SymbolName, ParameterDirection.Input)
						,new OracleParameter("paSymbolEnglishName",         OracleDbType.Varchar2, 8, eSD.SymbolEnglishName, ParameterDirection.Input)
						,new OracleParameter("paProductID",                 OracleDbType.Varchar2, 8, eSD.ProductID, ParameterDirection.Input)
						,new OracleParameter("paProductGrpID",              OracleDbType.Varchar2, 8, eSD.ProductGrpID, ParameterDirection.Input)
						,new OracleParameter("paSecurityGroupID",           OracleDbType.Varchar2, 8, eSD.SecurityGroupID, ParameterDirection.Input)
						,new OracleParameter("paPutOrCall",                 OracleDbType.Varchar2, 8, eSD.PutOrCall, ParameterDirection.Input)
						,new OracleParameter("paExerciseStyle",             OracleDbType.Varchar2, 8, eSD.ExerciseStyle, ParameterDirection.Input)
						,new OracleParameter("paMaturityMonthYear",         OracleDbType.Varchar2, 8, eSD.MaturityMonthYear, ParameterDirection.Input)
						,new OracleParameter("paMaturityDate",              OracleDbType.Varchar2, 8, eSD.MaturityDate, ParameterDirection.Input)
						,new OracleParameter("paIssuer",                    OracleDbType.Varchar2, 8, eSD.Issuer, ParameterDirection.Input)
						,new OracleParameter("paIssueDate",                 OracleDbType.Varchar2, 8, eSD.IssueDate, ParameterDirection.Input)
						,new OracleParameter("paContractMultiplier",        OracleDbType.Varchar2, 8, eSD.ContractMultiplier, ParameterDirection.Input)
						,new OracleParameter("paCouponRate",                OracleDbType.Varchar2, 8, eSD.CouponRate, ParameterDirection.Input)
						,new OracleParameter("paCurrency",                  OracleDbType.Varchar2, 8, eSD.Currency, ParameterDirection.Input)
						,new OracleParameter("paListedShares",              OracleDbType.Varchar2, 8, eSD.ListedShares, ParameterDirection.Input)
						,new OracleParameter("paHighLimitPrice",            OracleDbType.Varchar2, 8, eSD.HighLimitPrice, ParameterDirection.Input)
						,new OracleParameter("paLowLimitPrice",             OracleDbType.Varchar2, 8, eSD.LowLimitPrice, ParameterDirection.Input)
						,new OracleParameter("paStrikePrice",               OracleDbType.Varchar2, 8, eSD.StrikePrice, ParameterDirection.Input)
						,new OracleParameter("paSecurityStatus",            OracleDbType.Varchar2, 8, eSD.SecurityStatus, ParameterDirection.Input)
						,new OracleParameter("paContractSize",              OracleDbType.Varchar2, 8, eSD.ContractSize, ParameterDirection.Input)
						,new OracleParameter("paSettlMethod",               OracleDbType.Varchar2, 8, eSD.SettlMethod, ParameterDirection.Input)
						,new OracleParameter("paYield",                     OracleDbType.Varchar2, 8, eSD.Yield, ParameterDirection.Input)
						,new OracleParameter("paReferencePrice",            OracleDbType.Varchar2, 8, eSD.ReferencePrice, ParameterDirection.Input)
						,new OracleParameter("paEvaluationPrice",           OracleDbType.Varchar2, 8, eSD.EvaluationPrice, ParameterDirection.Input)
						,new OracleParameter("paHgstOrderPrice",            OracleDbType.Varchar2, 8, eSD.HgstOrderPrice, ParameterDirection.Input)
						,new OracleParameter("paLwstOrderPrice",            OracleDbType.Varchar2, 8, eSD.LwstOrderPrice, ParameterDirection.Input)
						,new OracleParameter("paPrevClosePx",               OracleDbType.Varchar2, 8, eSD.PrevClosePx, ParameterDirection.Input)
						,new OracleParameter("paSymbolCloseInfoPxType",     OracleDbType.Varchar2, 8, eSD.SymbolCloseInfoPxType, ParameterDirection.Input)
						,new OracleParameter("paFirstTradingDate",          OracleDbType.Varchar2, 8, eSD.FirstTradingDate, ParameterDirection.Input)
						,new OracleParameter("paFinalTradeDate",            OracleDbType.Varchar2, 8, eSD.FinalTradeDate, ParameterDirection.Input)
						,new OracleParameter("paFinalSettleDate",           OracleDbType.Varchar2, 8, eSD.FinalSettleDate, ParameterDirection.Input)
						,new OracleParameter("paListingDate",               OracleDbType.Varchar2, 8, eSD.ListingDate, ParameterDirection.Input)
						//,new OracleParameter("paOpenInterestQty",           OracleDbType.Varchar2, 8, eSD.OpenInterestQty, ParameterDirection.Input)
						//,new OracleParameter("paSettlementPrice",           OracleDbType.Varchar2, 8, eSD.SettlementPrice, ParameterDirection.Input)
						,new OracleParameter("paREtriggeringconditioncode", OracleDbType.Varchar2, 8, eSD.RandomEndTriggeringConditionCode, ParameterDirection.Input)
						,new OracleParameter("paExClassType",               OracleDbType.Varchar2, 8, eSD.ExClassType, ParameterDirection.Input)
						,new OracleParameter("paVWAP",                      OracleDbType.Varchar2, 8, eSD.VWAP, ParameterDirection.Input)
						,new OracleParameter("paSymbolAdminStatusCode",     OracleDbType.Varchar2, 8, eSD.SymbolAdminStatusCode, ParameterDirection.Input)
						,new OracleParameter("paSymbolTradingMethodSC",     OracleDbType.Varchar2, 8, eSD.SymbolTradingMethodStatusCode, ParameterDirection.Input)
						,new OracleParameter("paSymbolTradingSantionSC",    OracleDbType.Varchar2, 8, eSD.SymbolTradingSantionStatusCode, ParameterDirection.Input)
						,new OracleParameter("paCheckSum",                  OracleDbType.Varchar2, 8, eSD.CheckSum, ParameterDirection.Input)
					};

					// 2. main									
					result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

				}

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

                // return (neu sp ko tra error code,msg thi tu gan default)
                return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EDalResult.__STRING_SUCCESS, Data = result.Data };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._cS6GApp.ErrorLogger.LogErrorContext(ex, ec);
                // error => return null
                return new EDalResult() { Code = EGlobalConfig.__CODE_ERROR_IN_LAYER_DAL, Message = ex.Message, Data = null };
            }
        }
	}
}
