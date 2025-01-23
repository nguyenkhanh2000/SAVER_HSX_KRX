using BaseSqlServerLib.Implementations;
using BaseSqlServerLib.Interfaces;
using CommonLib.Interfaces;
using Dapper;
using MDDSCore.Messages;
using PriceLib.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using SystemCore.Entities;
using SystemCore.Temporaries;

namespace PriceLib.Implementations
{
	/// <summary>
	/// 2020-07-24 10:54:33 ngocta2
	/// insert MDDS vao db mssql
	/// </summary>
	public class CMDDSMssql : CBasePrice, IMDDS
	{

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="s6GApp"></param>
        /// <param name="ePriceConfig"></param>
        //private ISqlServer<ESecurityDefinition> sqlServer;
        //  private readonly SqlConnection _connectionSql;
        private readonly SemaphoreSlim semaphoreSQL = new SemaphoreSlim(1, 1);
        public CMDDSMssql(IS6GApp s6GApp, EPriceConfig ePriceConfig) : base(s6GApp, ePriceConfig)
		{
			// this._ePriceConfig.ConnectionMssql = connectionSql;
        }
        TimeZoneInfo timeZone = TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time"); // UTC+7
        /// <summary>
        /// 2020-08-04 14:20:48 ngocta2
        /// dung de exec bulk script, insert nhieu k rows vao db 1 luc
        /// </summary>
        /// <param name="script"></param>
        /// <returns></returns>
        public async Task<EDalResult> ExecuteScript(string script)
		{
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} script={script}", true);
			ISqlServer<ESecurityDefinition> sqlServer = new CSqlServer<ESecurityDefinition>(this._cS6GApp, this._ePriceConfig.ConnectionMssql);
			await semaphoreSQL.WaitAsync();
            try
			{
				EDalResult result;

                int affectedRowCount = await sqlServer.ExecuteAsync(script);
				result = new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EDalResult.__STRING_SUCCESS, Data = affectedRowCount };
				return result;
			}
			catch (Exception ex)
			{
				// log error + buffer data
				this._cS6GApp.ErrorLogger.LogErrorContext(ex, ec);
				// error => return null
				return new EDalResult() { Code = EGlobalConfig.__CODE_ERROR_IN_LAYER_DAL, Message = ex.Message, Data = null };
			}
			finally
			{
                semaphoreSQL.Release();

            }
		}

		/// <summary>
		/// 2020-07-24 10:55:58 ngocta2
		/// 4.1 Security Definition
		/// insert data vao table tSecurityDefinition
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eSD"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateSecurityDefinition(ESecurityDefinition eSD, bool getScriptOnly = false)
		{
			// log input
            TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eSD={this._cS6GApp.Common.SerializeObject(eSD)}", true);

            try
            {
				// 0. init
				ISqlServer<ESecurityDefinition> sqlServer = new CSqlServer<ESecurityDefinition>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateSecurityDefinition;
				EDalResult result;

				// process error string
				eSD.SymbolName = this._cS6GApp.Common.ProcessSqlEscapeChar(eSD.SymbolName);
				eSD.SymbolEnglishName = this._cS6GApp.Common.ProcessSqlEscapeChar(eSD.SymbolEnglishName);

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",                eSD.BeginString,                        DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",                 eSD.BodyLength,                         DbType.Int64,        ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",                    eSD.MsgType,                            DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",               eSD.SenderCompID,                       DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",               eSD.TargetCompID,                       DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",                  eSD.MsgSeqNum,                          DbType.Int64,        ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",                eSD.SendingTime,                        DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",                   eSD.MarketID,                           DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABOARDID}",                    eSD.BoardID,                            DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATOTNUMREPORTS}",              eSD.TotNumReports,                      DbType.Int64,        ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASECURITYEXCHANGE}",           eSD.SecurityExchange,                   DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",                     eSD.Symbol,                             DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATICKERCODE}",                 eSD.TickerCode,                         DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOLSHORTCODE}",            eSD.SymbolShortCode,                    DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOLNAME}",                 eSD.SymbolName,                         DbType.String,		ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOLENGLISHNAME}",          eSD.SymbolEnglishName,                  DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__APRODUCTID}",                  eSD.ProductID,                          DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__APRODUCTGRPID}",               eSD.ProductGrpID,                       DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASECURITYGROUPID}",            eSD.SecurityGroupID,                    DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__APUTORCALL}",                  eSD.PutOrCall,                          DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AEXERCISESTYLE}",              eSD.ExerciseStyle,                      DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMATURITYMONTHYEAR}",          eSD.MaturityMonthYear,                  DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMATURITYDATE}",               eSD.MaturityDate,                       DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AISSUER}",                     eSD.Issuer,                             DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AISSUEDATE}",                  eSD.IssueDate,                          DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACONTRACTMULTIPLIER}",         eSD.ContractMultiplier,                 DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACOUPONRATE}",                 eSD.CouponRate,                         DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACURRENCY}",                   eSD.Currency,                           DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ALISTEDSHARES}",               eSD.ListedShares,                       DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AHIGHLIMITPRICE}",             eSD.HighLimitPrice,                     DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ALOWLIMITPRICE}",              eSD.LowLimitPrice,                      DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASTRIKEPRICE}",                eSD.StrikePrice,                        DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASECURITYSTATUS}",             eSD.SecurityStatus,                     DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACONTRACTSIZE}",               eSD.ContractSize,                       DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASETTLMETHOD}",                eSD.SettlMethod,                        DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AYIELD}",                      eSD.Yield,                              DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AREFERENCEPRICE}",             eSD.ReferencePrice,                     DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AEVALUATIONPRICE}",            eSD.EvaluationPrice,                    DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AHGSTORDERPRICE}",             eSD.HgstOrderPrice,                     DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ALWSTORDERPRICE}",             eSD.LwstOrderPrice,                     DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__APREVCLOSEPX}",                eSD.PrevClosePx,                        DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOLCLOSEINFOPXTYPE}",      eSD.SymbolCloseInfoPxType,              DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFIRSTTRADINGDATE}",           eSD.FirstTradingDate,                   DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFINALTRADEDATE}",             eSD.FinalTradeDate,                     DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFINALSETTLEDATE}",            eSD.FinalSettleDate,                    DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ALISTINGDATE}",                eSD.ListingDate,                        DbType.AnsiString,   ParameterDirection.Input);
				//dynamicParameters.Add($"@{__AOPENINTERESTQTY}",            eSD.OpenInterestQty,                    DbType.AnsiString,   ParameterDirection.Input);
				//dynamicParameters.Add($"@{__ASETTLEMENTPRICE}",            eSD.SettlementPrice,                    DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ARETRIGGERINGCONDITIONCODE}",  eSD.RandomEndTriggeringConditionCode,   DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AEXCLASSTYPE}",                eSD.ExClassType,                        DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AVWAP}",                       eSD.VWAP,                               DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOLADMINSTATUSCODE}",      eSD.SymbolAdminStatusCode,              DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOLTRADINGMETHODSC}",      eSD.SymbolTradingMethodStatusCode,      DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOLTRADINGSANTIONSC}",     eSD.SymbolTradingSantionStatusCode,     DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",                   eSD.CheckSum,                           DbType.AnsiString,   ParameterDirection.Input);
                dynamicParameters.Add($"@{__ASECTORTYPECODE}",			   eSD.SectorTypeCode,					   DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__AREDUMPTIONDATE}",			   eSD.RedumptionDate,					   DbType.AnsiString, ParameterDirection.Input);

                // ko exec sp, chi lay script de run bulk update sau nay 
                if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				////this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-07-28 hungtq
		/// 4.2  Security Status
		/// insert data vao table tSecurityStatus
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eSS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateSecurityStatus(ESecurityStatus eSS, bool getScriptOnly= false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eSS={this._cS6GApp.Common.SerializeObject(eSS)}", true);

			try
			{
				// 0. init
				ISqlServer<ESecurityStatus> sqlServer = new CSqlServer<ESecurityStatus>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateSecurityStatus;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",       eSS.BeginString,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",        eSS.BodyLength,        DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",           eSS.MsgType,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",      eSS.SenderCompID,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",      eSS.TargetCompID,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",         eSS.MsgSeqNum,         DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",       eSS.SendingTime,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",          eSS.MarketID,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABOARDID}",           eSS.BoardID,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABOARDEVTID}",        eSS.BoardEvtID,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASESSOPENCLOSECODE}", eSS.SessOpenCloseCode, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",            eSS.Symbol,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRADINGSESSIONID}",  eSS.TradingSessionID,  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",          eSS.CheckSum,          DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__ATSCPRODGRPID}",	  eSS.TscProdGrpId,		 DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__AHALTRSNCODE}",		  eSS.HaltRsnCode,		 DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__APRODUCTID}",		  eSS.ProductID,		 DbType.AnsiString, ParameterDirection.Input);

                // ko exec sp, chi lay script de run bulk update sau nay 
                if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-07-28 hungtq
		/// 4.3 Security Information Notification
		/// insert data vao table tSecurityInformationNotification
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateSecurityInformationNotification(ESecurityInformationNotification eSIN , bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eSS={this._cS6GApp.Common.SerializeObject(eSIN)}", true);

			try
			{
				// 0. init
				ISqlServer<ESecurityInformationNotification> sqlServer = new CSqlServer<ESecurityInformationNotification>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateSecurityInformationNotification;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",     eSIN.BeginString,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",      eSIN.BodyLength,     DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",         eSIN.MsgType,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",    eSIN.SenderCompID,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",    eSIN.TargetCompID,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",       eSIN.MsgSeqNum,      DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",     eSIN.SendingTime,    DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",        eSIN.MarketID,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABOARDID}",         eSIN.BoardID,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",          eSIN.Symbol,         DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ALISTEDSHARES}",    eSIN.ListedShares,   DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AHIGHLIMITPRICE}",  eSIN.HighLimitPrice, DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ALOWLIMITPRICE}",   eSIN.LowLimitPrice,  DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AREFERENCEPRICE}",  eSIN.ReferencePrice, DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AHGSTORDERPRICE}",  eSIN.HgstOrderPrice, DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ALWSTORDERPRICE}",  eSIN.LwstOrderPrice, DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AEXCLASSTYPE}",     eSIN.ExClassType,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",        eSIN.CheckSum,       DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}


				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-07-28 hungtq
		/// 4.4 Security Symbol Closing Information
		/// insert data vao table tSymbolClosingInformation
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateSymbolClosingInformation(ESymbolClosingInformation eSCI, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eSS={this._cS6GApp.Common.SerializeObject(eSCI)}", true);

			try
			{
				// 0. init
				ISqlServer<ESymbolClosingInformation> sqlServer = new CSqlServer<ESymbolClosingInformation>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateSymbolClosingInformation;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",           eSCI.BeginString,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",            eSCI.BodyLength,            DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",               eSCI.MsgType,               DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",          eSCI.SenderCompID,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",          eSCI.TargetCompID,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",             eSCI.MsgSeqNum,             DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",           eSCI.SendingTime,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",              eSCI.MarketID,              DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABOARDID}",               eSCI.BoardID,               DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",                eSCI.Symbol,                DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOLCLOSEINFOPX}",     eSCI.SymbolCloseInfoPx,     DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOLCLOSEINFOPXTYPE}", eSCI.SymbolCloseInfoPxType, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOLCLOSEINFOYIELD}",  eSCI.SymbolCloseInfoYield,  DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",              eSCI.CheckSum,              DbType.AnsiString, ParameterDirection.Input);


				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-07-28 hungtq
		/// 4.5 Volatility Interruption
		/// insert data vao table tVolatilityInterruption
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateVolatilityInterruption(EVolatilityInterruption eVI ,bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eVI={this._cS6GApp.Common.SerializeObject(eVI)}", true);

			try
			{
				// 0. init
				ISqlServer<EVolatilityInterruption> sqlServer = new CSqlServer<EVolatilityInterruption>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateVolatilityInterruption;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",            eVI.BeginString,             DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",             eVI.BodyLength,              DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",                eVI.MsgType,                 DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",           eVI.SenderCompID,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",           eVI.TargetCompID,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",              eVI.MsgSeqNum,               DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",            eVI.SendingTime,             DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",               eVI.MarketID,                DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABOARDID}",                eVI.BoardID,                 DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",                 eVI.Symbol,                  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AVITYPECODE}",             eVI.VITypeCode,              DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AVIKINDCODE}",             eVI.VIKindCode,              DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASTATICVIBASEPRICE}",      eVI.StaticVIBasePrice,       DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ADYNAMICVIBASEPRICE}",     eVI.DynamicVIBasePrice,      DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AVIPRICE}",                eVI.VIPrice,                 DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASTATICVIDISPARTIYRATIO}", eVI.StaticVIDispartiyRatio,  DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASTATICVIDISPARTIYRATIO}", eVI.DynamicVIDispartiyRatio, DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",               eVI.CheckSum,                DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-07-28 hungtq
		/// 4.6 Market Maker Information
		/// insert data vao table tMarketMakerInformation
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eMMI"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateMarketMakerInformation(EMarketMakerInformation eMMI, bool getScriptOnly= false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eMMI={this._cS6GApp.Common.SerializeObject(eMMI)}", true);

			try
			{
				// 0. init
				ISqlServer<EMarketMakerInformation> sqlServer = new CSqlServer<EMarketMakerInformation>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateMarketMakerInformation;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",             eMMI.BeginString,             DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",              eMMI.BodyLength,              DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",                 eMMI.MsgType,                 DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",            eMMI.SenderCompID,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",            eMMI.TargetCompID,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",               eMMI.MsgSeqNum,               DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",             eMMI.SendingTime,             DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",                eMMI.MarketID,                DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETMAKERCONTRACTCODE}", eMMI.MarketMakerContractCode, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMEMBERNO}",                eMMI.MemberNo,                DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",                eMMI.CheckSum,                DbType.AnsiString, ParameterDirection.Input);


				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-07-28 hungtq
		/// 4.7 Symbol Event
		/// insert data vao table tSymbolEvent
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eSE"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateSymbolEvent(ESymbolEvent eSE, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eSE={this._cS6GApp.Common.SerializeObject(eSE)}", true);

			try
			{
				// 0. init
				ISqlServer<ESymbolEvent> sqlServer = new CSqlServer<ESymbolEvent>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateSymbolEvent;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",               eSE.BeginString,               DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",                eSE.BodyLength,                DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",                   eSE.MsgType,                   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",              eSE.SenderCompID,              DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",              eSE.TargetCompID,              DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",                 eSE.MsgSeqNum,                 DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",               eSE.SendingTime,               DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",                  eSE.MarketID,                  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",                    eSE.Symbol,                    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AEVENTKINDCODE}",             eSE.EventKindCode,             DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AEVENTOCCURRENCEREASONCODE}", eSE.EventOccurrenceReasonCode, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AEVENTSTARTDATE}",            eSE.EventStartDate,            DbType.Date,       ParameterDirection.Input);
				dynamicParameters.Add($"@{__AEVENTENDDATE}",              eSE.EventEndDate,              DbType.Date,       ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",                  eSE.CheckSum,                  DbType.AnsiString, ParameterDirection.Input);



				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-07-28 hungtq
		/// 4.8 Index Constituents Information
		/// insert data vao table tIndexConstituentsInformation
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eICI"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateIndexConstituentsInformation(EIndexConstituentsInformation eICI, bool getScriptOnly= false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eICI={this._cS6GApp.Common.SerializeObject(eICI)}", true);

			try
			{
				// 0. init
				ISqlServer<ESymbolEvent> sqlServer = new CSqlServer<ESymbolEvent>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateIndexConstituentsInformation;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",      eICI.BeginString,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",       eICI.BodyLength,       DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",          eICI.MsgType,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",     eICI.SenderCompID,     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",     eICI.TargetCompID,     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",        eICI.MsgSeqNum,        DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",      eICI.SendingTime,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",         eICI.MarketID,         DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETINDEXCLASS}", eICI.MarketIndexClass, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AINDEXSTYPECODE}",   eICI.IndexsTypeCode,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACURRENCY}",         eICI.Currency,         DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AIDXNAME}",          eICI.IdxName,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AIDXENGLISHNAME}",   eICI.IdxEnglishName,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATOTALMSGNO}",       eICI.TotalMsgNo,       DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACURRENTMSGNO}",     eICI.CurrentMsgNo,     DbType.Int64,      ParameterDirection.Input);
                dynamicParameters.Add($"@{__ASYMBOL}",			 eICI.Symbol, DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__ACHECKSUM}",         eICI.CheckSum,         DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-07-28 hungtq
		/// 4.9 Random End
		/// insert data vao table tRandomEnd
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eRE"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateRandomEnd(ERandomEnd eRE, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eICI={this._cS6GApp.Common.SerializeObject(eRE)}", true);

			try
			{
				// 0. init
				ISqlServer<ERandomEnd> sqlServer = new CSqlServer<ERandomEnd>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateRandomEnd;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}", eRE.BeginString, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}", eRE.BodyLength, DbType.Int64, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}", eRE.MsgType, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}", eRE.SenderCompID, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}", eRE.TargetCompID, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}", eRE.MsgSeqNum, DbType.Int64, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}", TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(eRE.SendingTime, "yyyy-MM-dd HH:mm:ss.fff", null), timeZone).ToString("yyyy-MM-dd HH:mm:ss.fff"), DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}", eRE.MarketID, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABOARDID}", eRE.BoardID, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}", eRE.Symbol, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRANSACTTIME}", eRE.TransactTime, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__RANDOMENDAPPLYCLASSIFICATION}", eRE.RandomEndApplyClassification, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__RANDOMENDTENTATIVEEXECUTIONPRICE}", eRE.RandomEndTentativeExecutionPrice, DbType.Decimal, ParameterDirection.Input);
				dynamicParameters.Add($"@{__RANDOMENDESTIMATEDHIGHESTPRICE}", eRE.RandomEndEstimatedHighestPrice, DbType.Decimal, ParameterDirection.Input);
				dynamicParameters.Add($"@{__RANDOMENDESTIMATEDHIGHESTPRICEDISPARATERATIO}", eRE.RandomEndEstimatedHighestPriceDisparateRatio, DbType.Decimal, ParameterDirection.Input);
				dynamicParameters.Add($"@{__RANDOMENDESTIMATEDLOWESTPRICE}", eRE.RandomEndEstimatedLowestPrice, DbType.Decimal, ParameterDirection.Input);
				dynamicParameters.Add($"@{__RANDOMENDESTIMATEDLOWESTPRICEDISPARATERATIO}", eRE.RandomEndEstimatedLowestPriceDisparateRatio, DbType.Decimal, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ALATESTPRICE}", eRE.LatestPrice, DbType.Decimal, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ALATESTPRICEDISPARATERATIO}", eRE.LatestPriceDisparateRatio, DbType.Decimal, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ARANDOMENDRELEASETIME}", eRE.RandomEndReleaseTimes, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}", eRE.CheckSum, DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-07-28 hungtq
		/// 4.10 Price
		/// insert data vao table tPrice
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eRE"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdatePriceAll(EPrice eP, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eP={this._cS6GApp.Common.SerializeObject(eP)}", true);

			try
			{
				// 0. init
			    ISqlServer<EPrice> sqlServer = new CSqlServer<EPrice>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdatePriceAll;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();

				dynamicParameters.Add($"@{__ABEGINSTRING}",        eP.BeginString,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",         eP.BodyLength,         DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",            eP.MsgType,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",       eP.SenderCompID,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",       eP.TargetCompID,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",          eP.MsgSeqNum,          DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",        eP.SendingTime,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",           eP.MarketID,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABOARDID}",            eP.BoardID,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRADINGSESSIONID}",   eP.TradingSessionID,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",             eP.Symbol,             DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRADEDATE}",          eP.TradeDate,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRANSACTTIME}",       eP.TransactTime,       DbType.AnsiString, ParameterDirection.Input);
				if (eP.TotalVolumeTraded != -9999999)
				{
					dynamicParameters.Add($"@{__ATOTALVOLUMETRADED}", eP.TotalVolumeTraded, DbType.Int64, ParameterDirection.Input);
				}
				
                dynamicParameters.Add($"@{__AGROSSTRADEAMT}",      eP.GrossTradeAmt,      DbType.Decimal,    ParameterDirection.Input);
                if (eP.BuyTotOrderQty != -9999999)
                {
                   
                    dynamicParameters.Add($"@{__ABUYTOTORDERQTY}", eP.BuyTotOrderQty, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYVALIDORDERCNT}", eP.BuyValidOrderCnt, DbType.Int64, ParameterDirection.Input);
                }
				if (eP.SellValidOrderCnt != -9999999)
				{
                    dynamicParameters.Add($"@{__ASELLTOTORDERQTY}", eP.SellTotOrderQty, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLVALIDORDERCNT}", eP.SellValidOrderCnt, DbType.Int64, ParameterDirection.Input);
					
				}

				dynamicParameters.Add($"@{__ANOMDENTRIES}",        eP.NoMDEntries,        DbType.Int64,      ParameterDirection.Input);
				
				// Mua
				if (eP.BuyPrice1 != -9999999 || eP.BuyPrice2 != -9999999 || eP.BuyPrice3 != -9999999)
				{
                    dynamicParameters.Add($"@{__ABUYPRICE1}", eP.BuyPrice1, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY1}", eP.BuyQuantity1, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE1_NOO}", eP.BuyPrice1_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE1_MDEY}", eP.BuyPrice1_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE1_MDEMMS}", eP.BuyPrice1_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE2}", eP.BuyPrice2, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY2}", eP.BuyQuantity2, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE2_NOO}", eP.BuyPrice2_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE2_MDEY}", eP.BuyPrice2_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE2_MDEMMS}", eP.BuyPrice2_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE3}", eP.BuyPrice3, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY3}", eP.BuyQuantity3, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE3_NOO}", eP.BuyPrice3_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE3_MDEY}", eP.BuyPrice3_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE3_MDEMMS}", eP.BuyPrice3_MDEMMS, DbType.Int64, ParameterDirection.Input);
                }

                if (eP.BuyPrice4 != -9999999 || eP.BuyPrice5 != -9999999 || eP.BuyPrice6 != -9999999 || eP.BuyPrice7 != -9999999 || eP.BuyPrice8 != -9999999 || eP.BuyPrice9 != -9999999 || eP.BuyPrice10 != -9999999)
                {
                    dynamicParameters.Add($"@{__ABUYPRICE4}", eP.BuyPrice4, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY4}", eP.BuyQuantity4, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE4_NOO}", eP.BuyPrice4_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE4_MDEY}", eP.BuyPrice4_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE4_MDEMMS}", eP.BuyPrice4_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE5}", eP.BuyPrice5, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY5}", eP.BuyQuantity5, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE5_NOO}", eP.BuyPrice5_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE5_MDEY}", eP.BuyPrice5_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE5_MDEMMS}", eP.BuyPrice5_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE6}", eP.BuyPrice6, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY6}", eP.BuyQuantity6, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE6_NOO}", eP.BuyPrice6_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE6_MDEY}", eP.BuyPrice6_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE6_MDEMMS}", eP.BuyPrice6_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE7}", eP.BuyPrice7, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY7}", eP.BuyQuantity7, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE7_NOO}", eP.BuyPrice7_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE7_MDEY}", eP.BuyPrice7_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE7_MDEMMS}", eP.BuyPrice7_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE8}", eP.BuyPrice8, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY8}", eP.BuyQuantity8, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE8_NOO}", eP.BuyPrice8_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE8_MDEY}", eP.BuyPrice8_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE8_MDEMMS}", eP.BuyPrice8_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE9}", eP.BuyPrice9, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY9}", eP.BuyQuantity9, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE9_NOO}", eP.BuyPrice9_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE9_MDEY}", eP.BuyPrice9_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE9_MDEMMS}", eP.BuyPrice9_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE10}", eP.BuyPrice10, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY10}", eP.BuyQuantity10, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE10_NOO}", eP.BuyPrice10_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE10_MDEY}", eP.BuyPrice10_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE10_MDEMMS}", eP.BuyPrice10_MDEMMS, DbType.Int64, ParameterDirection.Input);
                }


                //Bán
                if (eP.SellPrice1 != -9999999 || eP.SellPrice2 != -9999999 || eP.SellPrice3 != -9999999)
				{
                    dynamicParameters.Add($"@{__ASELLPRICE1}", eP.SellPrice1, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY1}", eP.SellQuantity1, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE1_NOO}", eP.SellPrice1_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE1_MDEY}", eP.SellPrice1_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE1_MDEMMS}", eP.SellPrice1_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE2}", eP.SellPrice2, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY2}", eP.SellQuantity2, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE2_NOO}", eP.SellPrice2_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE2_MDEY}", eP.SellPrice2_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE2_MDEMMS}", eP.SellPrice2_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE3}", eP.SellPrice3, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY3}", eP.SellQuantity3, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE3_NOO}", eP.SellPrice3_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE3_MDEY}", eP.SellPrice3_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE3_MDEMMS}", eP.SellPrice3_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    
                }

                if (eP.SellPrice4 != -9999999 || eP.SellPrice5 != -9999999 || eP.SellPrice6 != -9999999 || eP.SellPrice7 != -9999999 || eP.SellPrice8 != -9999999 || eP.SellPrice9 != -9999999 || eP.SellPrice10 != -9999999)
                {
                    dynamicParameters.Add($"@{__ASELLPRICE4}", eP.SellPrice4, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY4}", eP.SellQuantity4, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE4_NOO}", eP.SellPrice4_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE4_MDEY}", eP.SellPrice4_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE4_MDEMMS}", eP.SellPrice4_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE5}", eP.SellPrice5, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY5}", eP.SellQuantity5, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE5_NOO}", eP.SellPrice5_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE5_MDEY}", eP.SellPrice5_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE5_MDEMMS}", eP.SellPrice5_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE6}", eP.SellPrice6, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY6}", eP.SellQuantity6, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE6_NOO}", eP.SellPrice6_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE6_MDEY}", eP.SellPrice6_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE6_MDEMMS}", eP.SellPrice6_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE7}", eP.SellPrice7, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY7}", eP.SellQuantity7, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE7_NOO}", eP.SellPrice7_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE7_MDEY}", eP.SellPrice7_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE7_MDEMMS}", eP.SellPrice7_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE8}", eP.SellPrice8, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY8}", eP.SellQuantity8, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE8_NOO}", eP.SellPrice8_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE8_MDEY}", eP.SellPrice8_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE8_MDEMMS}", eP.SellPrice8_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE9}", eP.SellPrice9, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY9}", eP.SellQuantity9, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE9_NOO}", eP.SellPrice9_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE9_MDEY}", eP.SellPrice9_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE9_MDEMMS}", eP.SellPrice9_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE10}", eP.SellPrice10, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY10}", eP.SellQuantity10, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE10_NOO}", eP.SellPrice10_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE10_MDEY}", eP.SellPrice10_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE10_MDEMMS}", eP.SellPrice10_MDEMMS, DbType.Int64, ParameterDirection.Input);

                }
                if (eP.MatchPrice != -9999999)
				{
                    dynamicParameters.Add($"@{__AMATCHPRICE}", eP.MatchPrice, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__AMATCHQUANTITY}", eP.MatchQuantity, DbType.Int64, ParameterDirection.Input);
                }
				if (eP.OpenPrice != -9999999)
				{
                    dynamicParameters.Add($"@{__AOPENPRICE}", eP.OpenPrice, DbType.Decimal, ParameterDirection.Input);
                }
                if (eP.ClosePrice != -9999999)
                {
                    dynamicParameters.Add($"@{__ACLOSEPRICE}", eP.ClosePrice, DbType.Decimal, ParameterDirection.Input);
                }
                if (eP.HighestPrice != -9999999)
                {
                    dynamicParameters.Add($"@{__AHIGHESTPRICE}", eP.HighestPrice, DbType.Decimal, ParameterDirection.Input);
                }
                if (eP.LowestPrice != -9999999)
                {
                    dynamicParameters.Add($"@{__ALOWESTPRICE}", eP.LowestPrice, DbType.Decimal, ParameterDirection.Input);
                }
         				
				dynamicParameters.Add($"@{__REPEATINGDATAFIX}",	   eP.RepeatingDataFix,   DbType.AnsiString, ParameterDirection.Input); // 2020-09-18 13:52:28 ngocta2 - prc_S6G_PS_UpdatePriceIntraday
				dynamicParameters.Add($"@{__REPEATINGDATAJSON}",   eP.RepeatingDataJson,  DbType.AnsiString, ParameterDirection.Input); // 2020-09-18 13:52:28 ngocta2 - prc_S6G_PS_UpdatePriceIntraday
				dynamicParameters.Add($"@{__ACHECKSUM}",           eP.CheckSum,           DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-07-28 hungtq
		/// 4.13 Index
		/// insert data vao table tIndex
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eI"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateIndex(EIndex eI, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eI={this._cS6GApp.Common.SerializeObject(eI)}", true);

			try
			{
				// 0. init
				ISqlServer<EIndex> sqlServer = new CSqlServer<EIndex>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateIndex;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",                     eI.BeginString,                      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",                      eI.BodyLength,                       DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",                         eI.MsgType,                          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",                    eI.SenderCompID,                     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",                    eI.TargetCompID,                     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",                       eI.MsgSeqNum,                        DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",                     eI.SendingTime,                      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",                        eI.MarketID,                         DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRADINGSESSIONID}",                eI.TradingSessionID,                 DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETINDEXCLASS}",                eI.MarketIndexClass,                 DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__AINDEXSTYPECODE}",					eI.IndexsTypeCode, DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__ACURRENCY}",						eI.Currency, DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__ATRANSACTTIME}",                    eI.TransactTime,                     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRANSDATE}",                       eI.TransDate,                        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AVALUEINDEXES}",                    eI.ValueIndexes,                     DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATOTALVOLUMETRADED}",               eI.TotalVolumeTraded,                DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AGROSSTRADEAMT}",                   eI.GrossTradeAmt,                    DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACONTAUCTACCTRDVOL}",               eI.ContauctAccTrdvol,                DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACONTAUCTACCTRDVAL}",               eI.ContauctAccTrdval,                DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABLKTRDACCTRDVOL}",                 eI.BlktrdAccTrdvol,                  DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABLKTRDACCTRDVAL}",                 eI.BlktrdAccTrdval,                  DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFLUCTUATIONUPPERLIMITISSUECOUNT}", eI.FluctuationUpperLimitIssueCount,  DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFLUCTUATIONUPISSUECOUNT}",         eI.FluctuationUpIssueCount,          DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFLUCTUATIONSTEADINESSISSUECOUNT}", eI.FluctuationSteadinessIssueCount,  DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFLUCTUATIONDOWNISSUECOUNT}",       eI.FluctuationDownIssueCount,        DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFLUCTUATIONLOWERLIMITISSUECOUNT}", eI.FluctuationLowerLimitIssueCount,  DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFLUCTUATIONUPISSUEVOLUME}",        eI.FluctuationUpIssueVolume,         DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFLUCTUATIONDOWNISSUEVOLUME}",      eI.FluctuationDownIssueVolume,       DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFLUCTUATIONSTEADINESSISSUEVOLUME}",eI.FluctuationSteadinessIssueVolume, DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",                        eI.CheckSum,                         DbType.Int64,      ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq
		/// 4.14 Investor per Industry
		/// insert data vao table tInvestorperIndustry
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eP"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateInvestorperIndustry(EInvestorPerIndustry eIPI, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eIPI={this._cS6GApp.Common.SerializeObject(eIPI)}", true);

			try
			{
				// 0. init
				ISqlServer<EInvestorPerIndustry> sqlServer = new CSqlServer<EInvestorPerIndustry>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateInvestorperIndustry;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",            eIPI.BeginString,            DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",             eIPI.BodyLength,             DbType.Int64,        ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",                eIPI.MsgType,                DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",           eIPI.SenderCompID,           DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",           eIPI.TargetCompID,           DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",              eIPI.MsgSeqNum,              DbType.Int64,        ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",            eIPI.SendingTime,            DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",               eIPI.MarketID,               DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRANSACTTIME}",           eIPI.TransactTime,           DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETINDEXCLASS}",       eIPI.MarketIndexClass,       DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AINDEXSTYPECODE}",         eIPI.IndexsTypeCode,         DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACURRENCY}",               eIPI.Currency,               DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABONDCLASSIFICATIONCODE}", eIPI.BondClassificationCode, DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASECURITYGROUPID}",        eIPI.SecurityGroupID,        DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__AINVESTCODE}",             eIPI.InvestCode,             DbType.AnsiString,   ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASELLVOLUME}",             eIPI.SellVolume,             DbType.Int64,        ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASELLTRADEAMOUNT}",        eIPI.SellTradeAmount,        DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABUYVOLUME}",              eIPI.BuyVolume,              DbType.Int64,        ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABUYTRADEDAMOUNT}",        eIPI.BuyTradedAmount,        DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",               eIPI.CheckSum,               DbType.AnsiString,   ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.17 Investor per Symbol
		/// insert data vao table tInvestorperSymbol
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eP"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateInvestorperSymbol(EInvestorPerSymbol eIPS, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eIPS={this._cS6GApp.Common.SerializeObject(eIPS)}", true);

			try
			{
				// 0. init
				ISqlServer<EInvestorPerSymbol> sqlServer = new CSqlServer<EInvestorPerSymbol>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateInvestorperSymbol;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",     eIPS.BeginString,     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",      eIPS.BodyLength,      DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",         eIPS.MsgType,         DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",    eIPS.SenderCompID,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",    eIPS.TargetCompID,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",       eIPS.MsgSeqNum,       DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",     eIPS.SendingTime,     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",        eIPS.MarketID,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",          eIPS.Symbol,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AINVESTCODE}",      eIPS.InvestCode,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASELLVOLUME}",      eIPS.SellVolume,      DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASELLTRADEAMOUNT}", eIPS.SellTradeAmount, DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABUYVOLUME}",       eIPS.BuyVolume,       DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABUYTRADEDAMOUNT}", eIPS.BuyTradedAmount, DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",        eIPS.CheckSum,        DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.18 Top N Members per Symbol
		/// insert data vao table tInvestorperSymbol
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTopNMembersperSymbol(ETopNMembersPerSymbol eTNMPS, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eTNMPS={this._cS6GApp.Common.SerializeObject(eTNMPS)}", true);

			try
			{
				// 0. init
				ISqlServer<ETopNMembersPerSymbol> sqlServer = new CSqlServer<ETopNMembersPerSymbol>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateTopNMembersperSymbol;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",     eTNMPS.BeginString,     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",      eTNMPS.BodyLength,      DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",         eTNMPS.MsgType,         DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",    eTNMPS.SenderCompID,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",    eTNMPS.TargetCompID,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",       eTNMPS.MsgSeqNum,       DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",     eTNMPS.SendingTime,     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",        eTNMPS.MarketID,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",          eTNMPS.Symbol,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATOTNUMREPORTS}",   eTNMPS.TotNumReports,   DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASELLRANKSEQ}",     eTNMPS.SellRankSeq,     DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASELLMEMBERNO}",    eTNMPS.SellMemberNo,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABUYRANKSEQ}",      eTNMPS.BuyRankSeq,      DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABUYMEMBERNO}",     eTNMPS.BuyMemberNo,     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASELLVOLUME}",      eTNMPS.SellVolume,      DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASELLTRADEAMOUNT}", eTNMPS.SellTradeAmount, DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABUYVOLUME}",       eTNMPS.BuyVolume,       DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABUYTRADEDAMOUNT}", eTNMPS.BuyTradedAmount, DbType.Decimal,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",        eTNMPS.CheckSum,        DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.19 Open Interest
		/// insert data vao table tOpenInterest
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateOpenInterest(EOpenInterest eOI, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eOI={this._cS6GApp.Common.SerializeObject(eOI)}", true);

			try
			{
				// 0. init
				ISqlServer<EOpenInterest> sqlServer = new CSqlServer<EOpenInterest>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateOpenInterest;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",     eOI.BeginString,     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",      eOI.BodyLength,      DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",         eOI.MsgType,         DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",    eOI.SenderCompID,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",    eOI.TargetCompID,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",       eOI.MsgSeqNum,       DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",     eOI.SendingTime,     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",        eOI.MarketID,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",          eOI.Symbol,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRADEDATE}",       eOI.TradeDate,       DbType.Date,       ParameterDirection.Input);
				dynamicParameters.Add($"@{__AOPENINTERESTQTY}", eOI.OpenInterestQty, DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",        eOI.CheckSum,        DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.20 Deem Trade Price
		/// insert data vao table tDeemTradePrice
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateDeemTradePrice(EDeemTradePrice eDTP, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eDTP={this._cS6GApp.Common.SerializeObject(eDTP)}", true);

			try
			{
				// 0. init
				ISqlServer<EDeemTradePrice> sqlServer = new CSqlServer<EDeemTradePrice>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateDeemTradePrice;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",        eDTP.BeginString,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",         eDTP.BodyLength,         DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",            eDTP.MsgType,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",       eDTP.SenderCompID,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",       eDTP.TargetCompID,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",          eDTP.MsgSeqNum,          DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",        eDTP.SendingTime,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",           eDTP.MarketID,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABOARDID}",            eDTP.BoardID,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",             eDTP.Symbol,             DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AEXPECTEDTRADEPX}",    eDTP.ExpectedTradePx,    DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__AEXPECTEDTRADEQTY}",   eDTP.ExpectedTradeQty,   DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AEXPECTEDTRADEYIELD}", eDTP.ExpectedTradeYield, DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",           eDTP.CheckSum,           DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.21 Foreigner Order Limit
		/// insert data vao table tEForeignerOrderLimit
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateForeignerOrderLimit(EForeignerOrderLimit eFOL, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eFOL={this._cS6GApp.Common.SerializeObject(eFOL)}", true);

			try
			{
				// 0. init
				ISqlServer<EForeignerOrderLimit> sqlServer = new CSqlServer<EForeignerOrderLimit>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateForeignerOrderLimit;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",            eFOL.BeginString,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",             eFOL.BodyLength,             DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",                eFOL.MsgType,                DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",           eFOL.SenderCompID,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",           eFOL.TargetCompID,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",              eFOL.MsgSeqNum,              DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",            eFOL.SendingTime,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",               eFOL.MarketID,               DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",                 eFOL.Symbol,                 DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFOREIGNERBUYPOSBLQTY}",   eFOL.ForeignerBuyPosblQty,   DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFOREIGNERORDERLIMITQTY}", eFOL.ForeignerOrderLimitQty, DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",               eFOL.CheckSum,               DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.22 Price Limit Expansion
		/// insert data vao table tPriceLimitExpansion
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdatePriceLimitExpansion(EPriceLimitExpansion ePLE, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} ePLE={this._cS6GApp.Common.SerializeObject(ePLE)}", true);

			try
			{
				// 0. init
				ISqlServer<EPriceLimitExpansion> sqlServer = new CSqlServer<EPriceLimitExpansion>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdatePriceLimitExpansion;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",    ePLE.BeginString,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",     ePLE.BodyLength,     DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",        ePLE.MsgType,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",   ePLE.SenderCompID,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",   ePLE.TargetCompID,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",      ePLE.MsgSeqNum,      DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",    ePLE.SendingTime,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",       ePLE.MarketID,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABOARDID}",        ePLE.BoardID,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",         ePLE.Symbol,         DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRANSACTTIME}",   ePLE.TransactTime,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AHIGHLIMITPRICE}", ePLE.HighLimitPrice, DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ALOWLIMITPRICE}",  ePLE.LowLimitPrice,  DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",       ePLE.CheckSum,       DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.23 ETF iNav
		/// insert data vao table tETFiNav
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateETFiNav(EETFiNav eEiN, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eEiN={this._cS6GApp.Common.SerializeObject(eEiN)}", true);

			try
			{
				// 0. init
				ISqlServer<EETFiNav> sqlServer = new CSqlServer<EETFiNav>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateETFiNav;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",  eEiN.BeginString,  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",   eEiN.BodyLength,   DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",      eEiN.MsgType,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}", eEiN.SenderCompID, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}", eEiN.TargetCompID, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",    eEiN.MsgSeqNum,    DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",  eEiN.SendingTime,  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",     eEiN.MarketID,     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",       eEiN.Symbol,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRANSACTTIME}", eEiN.TransactTime, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AINAVVALUE}",    eEiN.iNAVvalue,    DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",     eEiN.CheckSum,     DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.24 ETF iIndex
		/// insert data vao table tETFiNav
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateETFiIndex(EETFiIndex eEiI, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eEiI={this._cS6GApp.Common.SerializeObject(eEiI)}", true);

			try
			{
				// 0. init
				ISqlServer<EETFiIndex> sqlServer = new CSqlServer<EETFiIndex>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateETFiIndex;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",  eEiI.BeginString,  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",   eEiI.BodyLength,   DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",      eEiI.MsgType,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}", eEiI.SenderCompID, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}", eEiI.TargetCompID, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",    eEiI.MsgSeqNum,    DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",  eEiI.SendingTime,  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",     eEiI.MarketID,     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",       eEiI.Symbol,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRANSACTTIME}", eEiI.TransactTime, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AVALUEINDEXES}", eEiI.ValueIndexes, DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",     eEiI.CheckSum,     DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.25 ETF TrackingError
		/// insert data vao table tETFTrackingError
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateETFTrackingError(EETFTrackingError eETE, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eETE={this._cS6GApp.Common.SerializeObject(eETE)}", true);

			try
			{
				// 0. init
				ISqlServer<EETFTrackingError> sqlServer = new CSqlServer<EETFTrackingError>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateETFTrackingError;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",    eETE.BeginString,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",     eETE.BodyLength,     DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",        eETE.MsgType,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",   eETE.SenderCompID,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",   eETE.TargetCompID,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",      eETE.MsgSeqNum,      DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",    eETE.SendingTime,    DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",       eETE.MarketID,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",         eETE.Symbol,         DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRADEDATE}",      eETE.TradeDate,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRACKINGERROR}",  eETE.TrackingError,  DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ADISPARATERATIO}", eETE.DisparateRatio, DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",       eETE.CheckSum,       DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.26 TopNSymbolswithTradinglQuantity
		/// insert data vao table tTopNSymbolswithTradinglQuantity
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNSWTQ"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTopNSymbolswithTradingQuantity(ETopNSymbolsWithTradingQuantity eTNSWTQ, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eTNSWTQ={this._cS6GApp.Common.SerializeObject(eTNSWTQ)}", true);

			try
			{
				// 0. init
				ISqlServer<ETopNSymbolsWithTradingQuantity> sqlServer = new CSqlServer<ETopNSymbolsWithTradingQuantity>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateTopNSymbolswithTradingQuantity;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",   eTNSWTQ.BeginString,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",    eTNSWTQ.BodyLength,    DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",       eTNSWTQ.MsgType,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",  eTNSWTQ.SenderCompID,  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",  eTNSWTQ.TargetCompID,  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",     eTNSWTQ.MsgSeqNum,     DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",   eTNSWTQ.SendingTime,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",      eTNSWTQ.MarketID,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATOTNUMREPORTS}", eTNSWTQ.TotNumReports, DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ARANK}",          eTNSWTQ.Rank,          DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",        eTNSWTQ.Symbol,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMDENTRYSIZE}",   eTNSWTQ.MDEntrySize,   DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",      eTNSWTQ.CheckSum,      DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.27 Top N Symbols with Current Price
		/// insert data vao table tTopNSymbolswithCurrentPrice
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNSWTQ"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTopNSymbolswithCurrentPrice(ETopNSymbolsWithCurrentPrice eTNSWCP, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eTNSWCP={this._cS6GApp.Common.SerializeObject(eTNSWCP)}", true);

			try
			{
				// 0. init
				ISqlServer<ETopNSymbolsWithCurrentPrice> sqlServer = new CSqlServer<ETopNSymbolsWithCurrentPrice>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateTopNSymbolswithCurrentPrice;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",   eTNSWCP.BeginString,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",    eTNSWCP.BodyLength,    DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",       eTNSWCP.MsgType,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",  eTNSWCP.SenderCompID,  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",  eTNSWCP.TargetCompID,  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",     eTNSWCP.MsgSeqNum,     DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",   eTNSWCP.SendingTime,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",      eTNSWCP.MarketID,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATOTNUMREPORTS}", eTNSWCP.TotNumReports, DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ARANK}",          eTNSWCP.Rank,          DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",        eTNSWCP.Symbol,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMDENTRYPX}",     eTNSWCP.MDEntryPx,     DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",      eTNSWCP.CheckSum,      DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.28 Top N Symbols with High Ratio of Price
		/// insert data vao table tTopNSymbolswithHighRatioofPrice
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNSWTQ"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTopNSymbolswithHighRatioofPrice(ETopNSymbolsWithHighRatioOfPrice eTNSWHROP, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eTNSWHROP={this._cS6GApp.Common.SerializeObject(eTNSWHROP)}", true);

			try
			{
				// 0. init
				ISqlServer<ETopNSymbolsWithHighRatioOfPrice> sqlServer = new CSqlServer<ETopNSymbolsWithHighRatioOfPrice>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateTopNSymbolswithHighRatioofPrice;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",           eTNSWHROP.BeginString,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",            eTNSWHROP.BodyLength,            DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",               eTNSWHROP.MsgType,               DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",          eTNSWHROP.SenderCompID,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",          eTNSWHROP.TargetCompID,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",             eTNSWHROP.MsgSeqNum,             DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",           eTNSWHROP.SendingTime,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",              eTNSWHROP.MarketID,              DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATOTNUMREPORTS}",         eTNSWHROP.TotNumReports,         DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ARANK}",                  eTNSWHROP.Rank,                  DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",                eTNSWHROP.Symbol,                DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__APRICEFLUCTUATIONRATIO}", eTNSWHROP.PriceFluctuationRatio, DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",              eTNSWHROP.CheckSum,              DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.29 Top N Symbols with Low Ratio of Price
		/// insert data vao table tTopNSymbolswithHighRatioofPrice
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNSWTQ"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTopNSymbolswithLowRatioofPrice(ETopNSymbolsWithLowRatioOfPrice eTNSWLROP, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eTNSWHROP={this._cS6GApp.Common.SerializeObject(eTNSWLROP)}", true);

			try
			{
				// 0. init
				ISqlServer<ETopNSymbolsWithLowRatioOfPrice> sqlServer = new CSqlServer<ETopNSymbolsWithLowRatioOfPrice>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateTopNSymbolswithLowRatioofPrice;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",           eTNSWLROP.BeginString,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",            eTNSWLROP.BodyLength,            DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",               eTNSWLROP.MsgType,               DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",          eTNSWLROP.SenderCompID,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",          eTNSWLROP.TargetCompID,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",             eTNSWLROP.MsgSeqNum,             DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",           eTNSWLROP.SendingTime,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",              eTNSWLROP.MarketID,              DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATOTNUMREPORTS}",         eTNSWLROP.TotNumReports,         DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ARANK}",                  eTNSWLROP.Rank,                  DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",                eTNSWLROP.Symbol,                DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__APRICEFLUCTUATIONRATIO}", eTNSWLROP.PriceFluctuationRatio, DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",              eTNSWLROP.CheckSum,              DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.30 Trading Result of Foreign Investors
		/// insert data vao table tTradingResultofForeignInvestors
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNSWTQ"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTradingResultofForeignInvestors(ETradingResultOfForeignInvestors eTRFI, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eTRFI={this._cS6GApp.Common.SerializeObject(eTRFI)}", true);

			try
			{
				// 0. init
				ISqlServer<ETradingResultOfForeignInvestors> sqlServer = new CSqlServer<ETradingResultOfForeignInvestors>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateTradingResultofForeignInvestors;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",          eTRFI.BeginString,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",           eTRFI.BodyLength,           DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",              eTRFI.MsgType,              DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",         eTRFI.SenderCompID,         DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",         eTRFI.TargetCompID,         DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",            eTRFI.MsgSeqNum,            DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",          eTRFI.SendingTime,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",             eTRFI.MarketID,             DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABOARDID}",              eTRFI.BoardID,              DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",               eTRFI.Symbol,               DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRADINGSESSIONID}",     eTRFI.TradingSessionID,     DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRANSACTTIME}",         eTRFI.TransactTime,         DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AFORNINVESTTYPECODE}",   eTRFI.FornInvestTypeCode,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASELLVOLUME}",           eTRFI.SellVolume,           DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASELLTRADEAMOUNT}",      eTRFI.SellTradeAmount,      DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABUYVOLUME}",            eTRFI.BuyVolume,            DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABUYTRADEDAMOUNT}",      eTRFI.BuyTradedAmount,      DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASELLVOLUMETOTAL}",      eTRFI.SellVolumeTotal,      DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASELLTRADEAMOUNTTOTAL}", eTRFI.SellTradeAmountTotal, DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABUYVOLUMETOTAL}",       eTRFI.BuyVolumeTotal,       DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABUYTRADEDAMOUNTTOTAL}", eTRFI.BuyTradeAmountTotal,  DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",             eTRFI.CheckSum,             DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.31 Disclosure
		/// insert data vao table tDisclosure
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNSWTQ"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateDisclosure(EDisclosure eD, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eD={this._cS6GApp.Common.SerializeObject(eD)}", true);

			try
			{
				// 0. init
				ISqlServer<EDisclosure> sqlServer = new CSqlServer<EDisclosure>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateDisclosure;
				EDalResult result;

				eD.SymbolName = this._cS6GApp.Common.ProcessSqlEscapeChar(eD.SymbolName);

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",           eD.BeginString,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",            eD.BodyLength,            DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",               eD.MsgType,               DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",          eD.SenderCompID,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",          eD.TargetCompID,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",             eD.MsgSeqNum,             DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",           eD.SendingTime,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",              eD.MarketID,              DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASECURITYEXCHANGE}",      eD.SecurityExchange,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",                eD.Symbol,                DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOLNAME}",            eD.SymbolName,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ADISCLOSUREID}",          eD.DisclosureID,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATOTALMSGNO}",            eD.TotalMsgNo,            DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACURRENTMSGNO}",          eD.CurrentMsgNo,          DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ALANQUAGECATEGORY}",      eD.LanquageCategory,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ADATACATEGORY}",          eD.DataCategory,          DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__APUBLICINFORMATIONDATE}", eD.PublicInformationDate, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRANSMISSIONDATE}",      eD.TransmissionDate,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__APROCESSTYPE}",           eD.ProcessType,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AHEADLINE}",              eD.Headline,              DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODY}",                  eD.Body,                  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",              eD.CheckSum,              DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
		/// 2020-08-15 hungtq 
		/// 4.32 Time Stamp Polling
		/// insert data vao table tTimeStampPolling
		/// chi insert 1 row 1 lan exec sp
		/// </summary>
		/// <param name="eTNSWTQ"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTimeStampPolling(ETimeStampPolling eTSP, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eD={this._cS6GApp.Common.SerializeObject(eTSP)}", true);

			try
			{
				// 0. init
				ISqlServer<ETimeStampPolling> sqlServer = new CSqlServer<ETimeStampPolling>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdateTimeStampPolling;
				EDalResult result;


				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",  eTSP.BeginString,  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",   eTSP.BodyLength,   DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",      eTSP.MsgType,      DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}", eTSP.SenderCompID, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}", eTSP.TargetCompID, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",    eTSP.MsgSeqNum,    DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}", TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(eTSP.SendingTime, "yyyy-MM-dd HH:mm:ss.fff", null), timeZone).ToString("yyyy-MM-dd HH:mm:ss.fff"),  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRANSACTTIME}", eTSP.TransactTime, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",     eTSP.CheckSum,     DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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
        /// 2024-01-25 anhnt 
        /// 4.? DrvProductEvent
        /// insert data vao table tDrvProductEvent
        /// </summary>
        /// <returns></returns>
        public async Task<EDalResult> UpdateDrvProductEventAll(EDrvProductEvent eDrvProductEvent, bool getScriptOnly = false)
        {
            // log input
            TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eDrvProductEvent={this._cS6GApp.Common.SerializeObject(eDrvProductEvent)}", true);
            try
            {
                // 0. init
                ISqlServer<EDrvProductEvent> sqlServer = new CSqlServer<EDrvProductEvent>(this._cS6GApp, this._ePriceConfig.ConnectionMssql);
                string spName = this._ePriceConfig.SpMddsMssqlEDrvProductEvent;
                EDalResult result;
                //  eD.SymbolName = this._cS6GApp.Common.ProcessSqlEscapeChar(eDrvProductEvent.SymbolName);
                // 1. input            
                DynamicParameters dynamicParameters = new DynamicParameters();
                dynamicParameters.Add($"@{__AMSGTYPE}", eDrvProductEvent.MsgType, DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__AMSGSEQNUM}", eDrvProductEvent.MsgSeqNum, DbType.Int64, ParameterDirection.Input);
                dynamicParameters.Add($"@{__ASENDINGTIME}", eDrvProductEvent.SendingTime, DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__ACREATEDATE}");
                dynamicParameters.Add($"@{__APRODUCTID}", eDrvProductEvent.ProductID, DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__AEVENTKINDCODE}", eDrvProductEvent.EventKindCode, DbType.Int64, ParameterDirection.Input);
                dynamicParameters.Add($"@{__AEVENTOCCURRENCEREASONCODE}", eDrvProductEvent.EventOccurrenceReasonCode, DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__AEVENTSTARTDATE}", eDrvProductEvent.EventStartDate, DbType.AnsiString, ParameterDirection.Input);
                dynamicParameters.Add($"@{__AEVENTENDDATE}", eDrvProductEvent.EventEndDate, DbType.AnsiString, ParameterDirection.Input);
                // ko exec sp, chi lay script de run bulk update sau nay 
                if (getScriptOnly)
                {
                    return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
                }
                // 2. main			
                result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);
                // log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
                //this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));
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
        /// 2020-07-28 hungtq
        /// 4.14 PriceRecovery
        /// insert data vao table tPriceRecovery
        /// chi insert 1 row 1 lan exec sp
        /// </summary>
        /// <param name="eRE"></param>
        /// <returns></returns>
        public async Task<EDalResult> UpdatePriceRecoveryAll(EPriceRecovery ePR, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} ePR={this._cS6GApp.Common.SerializeObject(ePR)}", true);

			try
			{
				// 0. init
				ISqlServer<EPriceRecovery> sqlServer = new CSqlServer<EPriceRecovery>(this._cS6GApp,  this._ePriceConfig.ConnectionMssql);
				string spName = this._ePriceConfig.SpMddsMssqlUpdatePriceRecoveryAll;
				EDalResult result;

				// 1. input            
				DynamicParameters dynamicParameters = new DynamicParameters();
				dynamicParameters.Add($"@{__ABEGINSTRING}",        ePR.BeginString,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABODYLENGTH}",         ePR.BodyLength,         DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGTYPE}",            ePR.MsgType,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDERCOMPID}",       ePR.SenderCompID,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATARGETCOMPID}",       ePR.TargetCompID,       DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMSGSEQNUM}",          ePR.MsgSeqNum,          DbType.Int64,      ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASENDINGTIME}",        ePR.SendingTime,        DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AMARKETID}",           ePR.MarketID,           DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ABOARDID}",            ePR.BoardID,            DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRADINGSESSIONID}",   ePR.TradingSessionID,   DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOL}",             ePR.Symbol,             DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__AOPNPX}",              ePR.OpnPx,              DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRDSESSNHIGHPX}",     ePR.TrdSessnHighPx,     DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ATRDSESSNLOWPX}",      ePR.TrdSessnLowPx,      DbType.Decimal,    ParameterDirection.Input);
				dynamicParameters.Add($"@{__ASYMBOLCLOSEINFOPX}",  ePR.SymbolCloseInfoPx,  DbType.Decimal,    ParameterDirection.Input);
				//dynamicParameters.Add($"@{__AOPNPXYLD}",           ePR.OpnPxYld,           DbType.Decimal,    ParameterDirection.Input);
				//dynamicParameters.Add($"@{__ATRDSESSNHIGHPXYLD}",  ePR.TrdSessnHighPxYld,  DbType.Decimal,    ParameterDirection.Input);
				//dynamicParameters.Add($"@{__ATRDSESSNLOWPXYLD}",   ePR.TrdSessnLowPxYld,   DbType.Decimal,    ParameterDirection.Input);
				//dynamicParameters.Add($"@{__ACLSPXYLD}",           ePR.ClsPxYld,           DbType.Decimal,    ParameterDirection.Input);
				if (ePR.TotalVolumeTraded != -9999999)
				{
                    dynamicParameters.Add($"@{__ATOTALVOLUMETRADED}", ePR.TotalVolumeTraded, DbType.Int64, ParameterDirection.Input);

                }
                dynamicParameters.Add($"@{__AGROSSTRADEAMT}",      ePR.GrossTradeAmt,      DbType.Decimal,    ParameterDirection.Input);
				if (ePR.SellTotOrderQty != -9999999)
				{
					dynamicParameters.Add($"@{__ASELLTOTORDERQTY}", ePR.SellTotOrderQty, DbType.Int64, ParameterDirection.Input);
					dynamicParameters.Add($"@{__ASELLVALIDORDERCNT}", ePR.SellValidOrderCnt, DbType.Int64, ParameterDirection.Input);
				}
				if (ePR.BuyTotOrderQty != -9999999)
				{
					dynamicParameters.Add($"@{__ABUYTOTORDERQTY}", ePR.BuyTotOrderQty, DbType.Int64, ParameterDirection.Input);
					dynamicParameters.Add($"@{__ABUYVALIDORDERCNT}", ePR.BuyValidOrderCnt, DbType.Int64, ParameterDirection.Input);
				}
				dynamicParameters.Add($"@{__ANOMDENTRIES}",        ePR.NoMDEntries,        DbType.Int64,      ParameterDirection.Input);
				if (ePR.BuyPrice1 != -9999999 || ePR.BuyPrice2 != -9999999 || ePR.BuyPrice10 != -9999999 || ePR.BuyPrice3 != -9999999 || ePR.BuyPrice4 != -9999999 || ePR.BuyPrice5 != -9999999 || ePR.BuyPrice6 != -9999999 || ePR.BuyPrice7 != -9999999 || ePR.BuyPrice8 != -9999999 || ePR.BuyPrice9 != -9999999 )
				{
                    dynamicParameters.Add($"@{__ABUYPRICE1}", ePR.BuyPrice1, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY1}", ePR.BuyQuantity1, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE1_NOO}", ePR.BuyPrice1_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE1_MDEY}", ePR.BuyPrice1_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE1_MDEMMS}", ePR.BuyPrice1_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE2}", ePR.BuyPrice2, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY2}", ePR.BuyQuantity2, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE2_NOO}", ePR.BuyPrice2_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE2_MDEY}", ePR.BuyPrice2_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE2_MDEMMS}", ePR.BuyPrice2_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE3}", ePR.BuyPrice3, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY3}", ePR.BuyQuantity3, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE3_NOO}", ePR.BuyPrice3_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE3_MDEY}", ePR.BuyPrice3_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE3_MDEMMS}", ePR.BuyPrice3_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE4}", ePR.BuyPrice4, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY4}", ePR.BuyQuantity4, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE4_NOO}", ePR.BuyPrice4_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE4_MDEY}", ePR.BuyPrice4_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE4_MDEMMS}", ePR.BuyPrice4_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE5}", ePR.BuyPrice5, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY5}", ePR.BuyQuantity5, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE5_NOO}", ePR.BuyPrice5_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE5_MDEY}", ePR.BuyPrice5_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE5_MDEMMS}", ePR.BuyPrice5_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE6}", ePR.BuyPrice6, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY6}", ePR.BuyQuantity6, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE6_NOO}", ePR.BuyPrice6_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE6_MDEY}", ePR.BuyPrice6_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE6_MDEMMS}", ePR.BuyPrice6_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE7}", ePR.BuyPrice7, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY7}", ePR.BuyQuantity7, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE7_NOO}", ePR.BuyPrice7_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE7_MDEY}", ePR.BuyPrice7_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE7_MDEMMS}", ePR.BuyPrice7_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE8}", ePR.BuyPrice8, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY8}", ePR.BuyQuantity8, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE8_NOO}", ePR.BuyPrice8_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE8_MDEY}", ePR.BuyPrice8_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE8_MDEMMS}", ePR.BuyPrice8_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE9}", ePR.BuyPrice9, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY9}", ePR.BuyQuantity9, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE9_NOO}", ePR.BuyPrice9_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE9_MDEY}", ePR.BuyPrice9_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE9_MDEMMS}", ePR.BuyPrice9_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE10}", ePR.BuyPrice10, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYQUANTITY10}", ePR.BuyQuantity10, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE10_NOO}", ePR.BuyPrice10_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE10_MDEY}", ePR.BuyPrice10_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ABUYPRICE10_MDEMMS}", ePR.BuyPrice10_MDEMMS, DbType.Int64, ParameterDirection.Input);

                }

				if (ePR.SellPrice1 != -9999999 || ePR.SellPrice2 != -9999999 || ePR.SellPrice3 != -9999999 || ePR.SellPrice4 != -9999999 || ePR.SellPrice5 != -9999999 || ePR.SellPrice6 != -9999999 || ePR.SellPrice7 != -9999999 || ePR.SellPrice8 != -9999999 || ePR.SellPrice9 != -9999999 || ePR.SellPrice10 != -9999999)
				{
                    dynamicParameters.Add($"@{__ASELLPRICE1}", ePR.SellPrice1, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY1}", ePR.SellQuantity1, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE1_NOO}", ePR.SellPrice1_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE1_MDEY}", ePR.SellPrice1_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE1_MDEMMS}", ePR.SellPrice1_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE2}", ePR.SellPrice2, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY2}", ePR.SellQuantity2, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE2_NOO}", ePR.SellPrice2_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE2_MDEY}", ePR.SellPrice2_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE2_MDEMMS}", ePR.SellPrice2_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE3}", ePR.SellPrice3, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY3}", ePR.SellQuantity3, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE3_NOO}", ePR.SellPrice3_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE3_MDEY}", ePR.SellPrice3_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE3_MDEMMS}", ePR.SellPrice3_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE4}", ePR.SellPrice4, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY4}", ePR.SellQuantity4, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE4_NOO}", ePR.SellPrice4_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE4_MDEY}", ePR.SellPrice4_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE4_MDEMMS}", ePR.SellPrice4_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE5}", ePR.SellPrice5, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY5}", ePR.SellQuantity5, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE5_NOO}", ePR.SellPrice5_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE5_MDEY}", ePR.SellPrice5_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE5_MDEMMS}", ePR.SellPrice5_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE6}", ePR.SellPrice6, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY6}", ePR.SellQuantity6, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE6_NOO}", ePR.SellPrice6_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE6_MDEY}", ePR.SellPrice6_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE6_MDEMMS}", ePR.SellPrice6_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE7}", ePR.SellPrice7, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY7}", ePR.SellQuantity7, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE7_NOO}", ePR.SellPrice7_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE7_MDEY}", ePR.SellPrice7_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE7_MDEMMS}", ePR.SellPrice7_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE8}", ePR.SellPrice8, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY8}", ePR.SellQuantity8, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE8_NOO}", ePR.SellPrice8_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE8_MDEY}", ePR.SellPrice8_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE8_MDEMMS}", ePR.SellPrice8_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE9}", ePR.SellPrice9, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY9}", ePR.SellQuantity9, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE9_NOO}", ePR.SellPrice9_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE9_MDEY}", ePR.SellPrice9_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE9_MDEMMS}", ePR.SellPrice9_MDEMMS, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE10}", ePR.SellPrice10, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLQUANTITY10}", ePR.SellQuantity10, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE10_NOO}", ePR.SellPrice10_NOO, DbType.Int64, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE10_MDEY}", ePR.SellPrice10_MDEY, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__ASELLPRICE10_MDEMMS}", ePR.SellPrice10_MDEMMS, DbType.Int64, ParameterDirection.Input);

                }
				if (ePR.MatchPrice != -9999999)
				{
                    dynamicParameters.Add($"@{__AMATCHPRICE}", ePR.MatchPrice, DbType.Decimal, ParameterDirection.Input);
                    dynamicParameters.Add($"@{__AMATCHQUANTITY}", ePR.MatchQuantity, DbType.Int64, ParameterDirection.Input);


                }
				if (ePR.OpenPrice != -9999999)
				{
					dynamicParameters.Add($"@{__AOPENPRICE}", ePR.OpenPrice, DbType.Decimal, ParameterDirection.Input);
				}
				if (ePR.ClosePrice != -9999999)
				{
					dynamicParameters.Add($"@{__ACLOSEPRICE}", ePR.ClosePrice, DbType.Decimal, ParameterDirection.Input);
				}
				if (ePR.HighestPrice != -9999999)
				{
					dynamicParameters.Add($"@{__AHIGHESTPRICE}", ePR.HighestPrice, DbType.Decimal, ParameterDirection.Input);
				}
				if (ePR.LowestPrice != -9999999)
				{
					dynamicParameters.Add($"@{__ALOWESTPRICE}", ePR.LowestPrice, DbType.Decimal, ParameterDirection.Input);
				}
				dynamicParameters.Add($"@{__REPEATINGDATAFIX}",    ePR.RepeatingDataFix,  DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__REPEATINGDATAJSON}",   ePR.RepeatingDataJson, DbType.AnsiString, ParameterDirection.Input);
				dynamicParameters.Add($"@{__ACHECKSUM}",           ePR.CheckSum,           DbType.AnsiString, ParameterDirection.Input);

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = sqlServer.GetScript(spName, dynamicParameters) };
				}

				// 2. main			
				result = await sqlServer.ExecuteSpNoQueryAsync(spName, dynamicParameters);

				// log sql afer (output) => D:\WebLog\StockGateway.NETCore\DSMarketWatchLib.Tests\SQL\20191126\058C108101.js
				//this._cS6GApp.SqlLogger.LogSqlSub(ec, EPriceConfig.__LOG_SQL_FILENAME, "sp: " + spName + " ==>  " + this._cS6GApp.Common.SerializeObject(result));

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

        public Task<EDalResult> UpdateSecurityDefinitionIG3SI(ESecurityDefinition eSecurityDefinition, bool getScriptOnly = false)
        {
            throw new NotImplementedException();
        }

        public Task<EDalResult> UpdatePriceAllIG3SI(EPrice ePrice, bool getScriptOnly = false)
        {
            throw new NotImplementedException();
        }
    }
}
