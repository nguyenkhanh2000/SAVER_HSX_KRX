using BaseOracleLib.Implementations;
using BaseOracleLib.Interfaces;
using CommonLib.Interfaces;
using MDDSCore.Messages;
using Oracle.ManagedDataAccess.Client;
using PriceLib.Interfaces;
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
	/// 2020-07-24 10:54:33 ngocta2
	/// insert MDDS vao db oracle
	/// </summary>
	public class CMDDSOracle : CBasePrice, IMDDS
	{
		Dictionary<string, string> symbolcode = new Dictionary<string, string>();
		private IOracle oracle;
        private readonly OracleConnection _connectionOracle;
        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="s6GApp"></param>
        /// <param name="ePriceConfig"></param>
        public CMDDSOracle(IS6GApp s6GApp, EPriceConfig ePriceConfig) : base(s6GApp, ePriceConfig)
		{
			//this._ePriceConfig.ConnectionOracle = connectionOracle;
        }
        TimeZoneInfo timeZone = TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time"); // UTC+7
        /*
		DECLARE
			preturncode   NUMBER := -1;
			preturnmess   VARCHAR2 (500) := 'INIT';
		BEGIN
			s6g.sp_tsecuritydefinition_insert ('FIX.4.4','519','d','VNMGW','99999','1','2019-05-17 02:14:26.128','STO','G4','3851','HO','VN000000KMR2','KMR','1729','MIRAE Joint Stock Company','MIRAE Joint Stock Company','S1STOST','STO','ST','','','','','ID00000083','','1','0','VND','1366000000','999999999','-999999999','0','N','1','','0','4930','0','0','0','4930','3','','','','20170101','0','0','2','00','0','NRM','SNE','NRM','056',pReturnCode,pReturnMess);
			s6g.sp_tsecuritydefinition_insert ('FIX.4.4','519','d','VNMGW','99999','1','2019-05-17 02:14:26.128','STO','G4','3851','HO','VN000000KMR2','KMR','1729','MIRAE Joint Stock Company','MIRAE Joint Stock Company','S1STOST','STO','ST','','','','','ID00000083','','1','0','VND','1366000000','999999999','-999999999','0','N','1','','0','4930','0','0','0','4930','3','','','','20170101','0','0','2','00','0','NRM','SNE','NRM','056',pReturnCode,pReturnMess);
			s6g.sp_tsecuritydefinition_insert ('FIX.4.4','519','d','VNMGW','99999','1','2019-05-17 02:14:26.128','STO','G4','3851','HO','VN000000KMR2','KMR','1729','MIRAE Joint Stock Company','MIRAE Joint Stock Company','S1STOST','STO','ST','','','','','ID00000083','','1','0','VND','1366000000','999999999','-999999999','0','N','1','','0','4930','0','0','0','4930','3','','','','20170101','0','0','2','00','0','NRM','SNE','NRM','056',pReturnCode,pReturnMess);
			s6g.sp_tsecuritydefinition_insert ('FIX.4.4','519','d','VNMGW','99999','1','2019-05-17 02:14:26.128','STO','G4','3851','HO','VN000000KMR2','KMR','1729','MIRAE Joint Stock Company','MIRAE Joint Stock Company','S1STOST','STO','ST','','','','','ID00000083','','1','0','VND','1366000000','999999999','-999999999','0','N','1','','0','4930','0','0','0','4930','3','','','','20170101','0','0','2','00','0','NRM','SNE','NRM','056',pReturnCode,pReturnMess);
		END;
		*/
        /// <summary>
        /// 2020-08-05 09:21:43 ngocta2
        /// insert nhieu row 1 luc vao db oracle
        /// </summary>
        /// <param name="script"></param>
        /// <returns></returns>
        public async Task<EDalResult> ExecuteScript(string script)
		{
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} script={script}", true);
			IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
			EDalResult result;
			try
			{				
				result = await oracle.ExecuteAsync(script);				
				return result;
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
		/// 2020-07-24 10:55:58 ngocta2
		/// 4.1 Security Definition
		/// insert data vao table tSecurityDefinition
		/// chi insert 1 row 1 lan exec sp
		/// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eSD"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateSecurityDefinition(ESecurityDefinition eSD, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eSD={_cS6GApp.Common.SerializeObject(eSD)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);				
				string spName = this._ePriceConfig.SpMddsOracleUpdateSecurityDefinition;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
				     new OracleParameter($"p{__ABEGINSTRING}",                OracleDbType.NVarchar2,  eSD.BeginString,                      ParameterDirection.Input)
					,new OracleParameter($"p{__ABODYLENGTH}",                 OracleDbType.Int64,      eSD.BodyLength,                       ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",                    OracleDbType.NVarchar2,  eSD.MsgType,                          ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",               OracleDbType.NVarchar2,  eSD.SenderCompID,                     ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",               OracleDbType.NVarchar2,  eSD.TargetCompID,                     ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",                  OracleDbType.Int64,      eSD.MsgSeqNum,                        ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",                OracleDbType.NVarchar2,  eSD.SendingTime,                      ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",                   OracleDbType.NVarchar2,  eSD.MarketID,                         ParameterDirection.Input)
					,new OracleParameter($"p{__ABOARDID}",                    OracleDbType.NVarchar2,  eSD.BoardID,                          ParameterDirection.Input)
					,new OracleParameter($"p{__ATOTNUMREPORTS}",              OracleDbType.Int64,      eSD.TotNumReports,                    ParameterDirection.Input)
					,new OracleParameter($"p{__ASECURITYEXCHANGE}",           OracleDbType.NVarchar2,  eSD.SecurityExchange,                 ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",                     OracleDbType.NVarchar2,  eSD.Symbol,                           ParameterDirection.Input)
					,new OracleParameter($"p{__ATICKERCODE}",                 OracleDbType.NVarchar2,  eSD.TickerCode,                       ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOLSHORTCODE}",            OracleDbType.NVarchar2,  eSD.SymbolShortCode,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOLNAME}",                 OracleDbType.NVarchar2,  eSD.SymbolName.Replace("'","*"),                       ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOLENGLISHNAME}",          OracleDbType.Varchar2,   eSD.SymbolEnglishName.Replace("'","*"),                ParameterDirection.Input)
					,new OracleParameter($"p{__APRODUCTID}",                  OracleDbType.NVarchar2,  eSD.ProductID,                        ParameterDirection.Input)
					,new OracleParameter($"p{__APRODUCTGRPID}",               OracleDbType.NVarchar2,  eSD.ProductGrpID,                     ParameterDirection.Input)
					,new OracleParameter($"p{__ASECURITYGROUPID}",            OracleDbType.NVarchar2,  eSD.SecurityGroupID,                  ParameterDirection.Input)
					,new OracleParameter($"p{__APUTORCALL}",                  OracleDbType.NVarchar2,  eSD.PutOrCall,                        ParameterDirection.Input)
					,new OracleParameter($"p{__AEXERCISESTYLE}",              OracleDbType.NVarchar2,  eSD.ExerciseStyle,                    ParameterDirection.Input)
					,new OracleParameter($"p{__AMATURITYMONTHYEAR}",          OracleDbType.NVarchar2,  eSD.MaturityMonthYear,                ParameterDirection.Input)
					,new OracleParameter($"p{__AMATURITYDATE}",               OracleDbType.NVarchar2,  eSD.MaturityDate,                     ParameterDirection.Input)
					,new OracleParameter($"p{__AISSUER}",                     OracleDbType.NVarchar2,  eSD.Issuer,                           ParameterDirection.Input)
					,new OracleParameter($"p{__AISSUEDATE}",                  OracleDbType.NVarchar2,  eSD.IssueDate,                        ParameterDirection.Input)
					,new OracleParameter($"p{__ACONTRACTMULTIPLIER}",         OracleDbType.Decimal,    eSD.ContractMultiplier,               ParameterDirection.Input)
					,new OracleParameter($"p{__ACOUPONRATE}",                 OracleDbType.Decimal,    eSD.CouponRate,                       ParameterDirection.Input)
					,new OracleParameter($"p{__ACURRENCY}",                   OracleDbType.NVarchar2,  eSD.Currency,                         ParameterDirection.Input)
					,new OracleParameter($"p{__ALISTEDSHARES}",               OracleDbType.Int64,      eSD.ListedShares,                     ParameterDirection.Input)
					,new OracleParameter($"p{__AHIGHLIMITPRICE}",             OracleDbType.Decimal,    eSD.HighLimitPrice,                   ParameterDirection.Input)
					,new OracleParameter($"p{__ALOWLIMITPRICE}",              OracleDbType.Decimal,    eSD.LowLimitPrice,                    ParameterDirection.Input)
					,new OracleParameter($"p{__ASTRIKEPRICE}",                OracleDbType.Decimal,    eSD.StrikePrice,                      ParameterDirection.Input)
					,new OracleParameter($"p{__ASECURITYSTATUS}",             OracleDbType.NVarchar2,  eSD.SecurityStatus,                   ParameterDirection.Input)
					,new OracleParameter($"p{__ACONTRACTSIZE}",               OracleDbType.Decimal,    eSD.ContractSize,                     ParameterDirection.Input)
					,new OracleParameter($"p{__ASETTLMETHOD}",                OracleDbType.NVarchar2,  eSD.SettlMethod,                      ParameterDirection.Input)
					,new OracleParameter($"p{__AYIELD}",                      OracleDbType.Decimal,    eSD.Yield,                            ParameterDirection.Input)
					,new OracleParameter($"p{__AREFERENCEPRICE}",             OracleDbType.Decimal,    eSD.ReferencePrice,                   ParameterDirection.Input)
					,new OracleParameter($"p{__AEVALUATIONPRICE}",            OracleDbType.Decimal,    eSD.EvaluationPrice,                  ParameterDirection.Input)
					,new OracleParameter($"p{__AHGSTORDERPRICE}",             OracleDbType.Decimal,    eSD.HgstOrderPrice,                   ParameterDirection.Input)
					,new OracleParameter($"p{__ALWSTORDERPRICE}",             OracleDbType.Decimal,    eSD.LwstOrderPrice,                   ParameterDirection.Input)
					,new OracleParameter($"p{__APREVCLOSEPX}",                OracleDbType.Decimal,    eSD.PrevClosePx,                      ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOLCLOSEINFOPXTYPE}",      OracleDbType.NVarchar2,  eSD.SymbolCloseInfoPxType,            ParameterDirection.Input)
					,new OracleParameter($"p{__AFIRSTTRADINGDATE}",           OracleDbType.NVarchar2,  eSD.FirstTradingDate,                 ParameterDirection.Input)
					,new OracleParameter($"p{__AFINALTRADEDATE}",             OracleDbType.NVarchar2,  eSD.FinalTradeDate,                   ParameterDirection.Input)
					,new OracleParameter($"p{__AFINALSETTLEDATE}",            OracleDbType.NVarchar2,  eSD.FinalSettleDate,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ALISTINGDATE}",                OracleDbType.NVarchar2,  eSD.ListingDate,                      ParameterDirection.Input)
					//,new OracleParameter($"p{__AOPENINTERESTQTY}",            OracleDbType.NVarchar2,  eSD.OpenInterestQty,                  ParameterDirection.Input)
					//,new OracleParameter($"p{__ASETTLEMENTPRICE}",            OracleDbType.Decimal,    eSD.SettlementPrice,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ARETRIGGERINGCONDITIONCODE}",  OracleDbType.NVarchar2,  eSD.RandomEndTriggeringConditionCode, ParameterDirection.Input)
					,new OracleParameter($"p{__AEXCLASSTYPE}",                OracleDbType.NVarchar2,  eSD.ExClassType,                      ParameterDirection.Input)
					,new OracleParameter($"p{__AVWAP}",                       OracleDbType.Decimal,    eSD.VWAP,                             ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOLADMINSTATUSCODE}",      OracleDbType.NVarchar2,  eSD.SymbolAdminStatusCode,            ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOLTRADINGMETHODSC}",      OracleDbType.NVarchar2,  eSD.SymbolTradingMethodStatusCode,    ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOLTRADINGSANTIONSC}",     OracleDbType.NVarchar2,  eSD.SymbolTradingSantionStatusCode,   ParameterDirection.Input)
                    ,new OracleParameter($"p{__ASECTORTYPECODE}",             OracleDbType.Varchar2,   eSD.SectorTypeCode,					 ParameterDirection.Input)
                    ,new OracleParameter($"p{__AREDUMPTIONDATE}",             OracleDbType.Varchar2,   eSD.RedumptionDate,				 ParameterDirection.Input)
                    ,new OracleParameter($"p{__ACHECKSUM}",                   OracleDbType.NVarchar2,  eSD.CheckSum,                         ParameterDirection.Input)
                    ,new OracleParameter($"p{__RETURNCODE}",                  OracleDbType.Int64,      null,                                 ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",                  OracleDbType.NVarchar2,  500, null,                            ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
        //INSERT TBL_IG3_SI
     /*   public async Task<EDalResult> UpdateSecurityDefinitionIG3SI(ESecurityDefinition eSD, bool getScriptOnly = false)
        {
            // log input
            TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eSD={_cS6GApp.Common.SerializeObject(eSD)}", true);

            try
            {
                // 0. init
                IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle2);
                string spName = this._ePriceConfig.SpMddsOracleUpdatePriceIG3SI;
                EDalResult result = null;
              
                string code = "";
				if (eSD.MarketID == "STX")
				{
					code = "LIS_BRD_01";
				}else if (eSD.MarketID == "UPX")
				{
                    code = "UPC_BRD_01";
                }
				else
				{
                    code = "DER_BRD_01";
                }
				
				if (!symbolcode.ContainsKey(eSD.Symbol))
				{
                    symbolcode.Add(eSD.Symbol, eSD.TickerCode);
                }
                // 1. input            

                OracleParameter[] paramArrary = new OracleParameter[]
                {
                     new OracleParameter($"p{__PID}",                         OracleDbType.Int64,  eSD.MsgSeqNum,                      ParameterDirection.Input)
                    ,new OracleParameter($"p{__PSYMBOL}",                     OracleDbType.NVarchar2,     eSD.TickerCode,              ParameterDirection.Input)
                    ,new OracleParameter($"p{__PBOARDCODE}",                  OracleDbType.NVarchar2,  code,                           ParameterDirection.Input)
                    ,new OracleParameter($"p{__PSECURITYTYPE}",               OracleDbType.NVarchar2,  eSD.SecurityGroupID,            ParameterDirection.Input)
                    ,new OracleParameter($"p{__PBASICPRICE}",                 OracleDbType.Decimal,  eSD.ReferencePrice,               ParameterDirection.Input)
                    ,new OracleParameter($"p{__PMATCHPRICE}",                 OracleDbType.Decimal,  null,                             ParameterDirection.Input)
                    ,new OracleParameter($"p{__POPENPRICE}",                  OracleDbType.Decimal,  eSD.PrevClosePx,                  ParameterDirection.Input)
                    ,new OracleParameter($"p{__PCLOSEPRICE}",                 OracleDbType.Decimal,  null,                             ParameterDirection.Input)
                    ,new OracleParameter($"p{__PMIDPX}",                      OracleDbType.Decimal,  eSD.VWAP,                         ParameterDirection.Input)
                    ,new OracleParameter($"p{__PHIGHESTPRICE}",               OracleDbType.Decimal,     eSD.HighLimitPrice,            ParameterDirection.Input)
                    ,new OracleParameter($"p{__PLOWESTPRICE}",                OracleDbType.Decimal,  eSD.LowLimitPrice,                ParameterDirection.Input)
                    ,new OracleParameter($"p{__PNM_TOTALTRADEDQTTY}",         OracleDbType.Decimal,  null,                             ParameterDirection.Input)                                   
                    ,new OracleParameter($"p{__RETURNCODE}",                  OracleDbType.Int64,     null,                            ParameterDirection.Output)
                    ,new OracleParameter($"p{__RETURNMESS}",                  OracleDbType.Varchar2,  500, null,                       ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

                // ko exec sp, chi lay script de run bulk update sau nay 
                if (getScriptOnly)
                {
                    return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
                }

                // 2. main									
                result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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

        public async Task<EDalResult> UpdatePriceAllIG3SI(EPrice eP, bool getScriptOnly = false)
        {
            // log input
            TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eP={_cS6GApp.Common.SerializeObject(eP)}", true);

            try
            {
                // 0. init
                IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle2);
                string spName = this._ePriceConfig.SpMddsOracleUpdatePriceIG3SI;
                EDalResult result = null;

				string symbol = "";
				if( symbolcode.ContainsKey(eP.Symbol))
				{
					symbol = symbolcode[eP.Symbol];

                }
                // 1. input            

                OracleParameter[] paramArrary = new OracleParameter[]
                {
                     new OracleParameter($"p{__PID}",                         OracleDbType.Int64,  eP.MsgSeqNum,                              ParameterDirection.Input)
                    ,new OracleParameter($"p{__PSYMBOL}",                     OracleDbType.NVarchar2,    symbol,                              ParameterDirection.Input)
                    ,new OracleParameter($"p{__PBOARDCODE}",                  OracleDbType.NVarchar2,  null,                                  ParameterDirection.Input)
                    ,new OracleParameter($"p{__PSECURITYTYPE}",               OracleDbType.NVarchar2,  null,                                  ParameterDirection.Input)
                    ,new OracleParameter($"p{__PBASICPRICE}",                 OracleDbType.Decimal,  null,                                    ParameterDirection.Input)
                    ,new OracleParameter($"p{__PMATCHPRICE}",                 OracleDbType.Decimal,  eP.MatchPrice,                           ParameterDirection.Input)
                    ,new OracleParameter($"p{__POPENPRICE}",                  OracleDbType.Decimal,  eP.OpenPrice,                            ParameterDirection.Input)
                    ,new OracleParameter($"p{__PCLOSEPRICE}",                 OracleDbType.Decimal,  eP.ClosePrice,                           ParameterDirection.Input)
                    ,new OracleParameter($"p{__PMIDPX}",                      OracleDbType.Decimal,  null,                                    ParameterDirection.Input)
                    ,new OracleParameter($"p{__PHIGHESTPRICE}",               OracleDbType.Decimal,     eP.HighestPrice,                      ParameterDirection.Input)
                    ,new OracleParameter($"p{__PLOWESTPRICE}",                OracleDbType.Decimal,  eP.LowestPrice,                          ParameterDirection.Input)
                    ,new OracleParameter($"p{__PNM_TOTALTRADEDQTTY}",         OracleDbType.Decimal,  eP.TotalVolumeTraded,                    ParameterDirection.Input)
                    ,new OracleParameter($"p{__RETURNCODE}",                  OracleDbType.Int64,     null,                                   ParameterDirection.Output)
                    ,new OracleParameter($"p{__RETURNMESS}",                  OracleDbType.Varchar2,  500, null,                              ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

                // ko exec sp, chi lay script de run bulk update sau nay 
                if (getScriptOnly)
                {
                    return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
                }

                // 2. main									
                result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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

*/
        /// <summary>
        /// 2020-07-28 hungtq
        /// 4.2  Security Status
        /// insert data vao table tSecurityStatus
        /// ============================================
        /// chu y phai khai bao size cua output var ten la "pReturnMess"
        /// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
        /// </summary>
        /// <param name="eSS"></param>
        /// <returns></returns>
        public async Task<EDalResult> UpdateSecurityStatus(ESecurityStatus eSS , bool getScriptOnly= false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eSS={_cS6GApp.Common.SerializeObject(eSS)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateSecurityStatus;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",       OracleDbType.Varchar2,  eSS.BeginString,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABODYLENGTH}",        OracleDbType.Int64,     eSS.BodyLength,        ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",           OracleDbType.Varchar2,  eSS.MsgType,           ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",      OracleDbType.Varchar2,  eSS.SenderCompID,      ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",      OracleDbType.Varchar2,  eSS.TargetCompID,      ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",         OracleDbType.Int64,     eSS.MsgSeqNum,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",       OracleDbType.Varchar2,  eSS.SendingTime,       ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",          OracleDbType.Varchar2,  eSS.MarketID,          ParameterDirection.Input)
					,new OracleParameter($"p{__ABOARDID}",           OracleDbType.Varchar2,  eSS.BoardID,           ParameterDirection.Input)
					,new OracleParameter($"p{__ABOARDEVTID}",        OracleDbType.Varchar2,  eSS.BoardEvtID,        ParameterDirection.Input)
					,new OracleParameter($"p{__ASESSOPENCLOSECODE}", OracleDbType.Varchar2,  eSS.SessOpenCloseCode, ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",            OracleDbType.Varchar2,  eSS.Symbol,            ParameterDirection.Input)
					,new OracleParameter($"p{__ATRADINGSESSIONID}",  OracleDbType.Varchar2,  eSS.TradingSessionID,  ParameterDirection.Input)
					,new OracleParameter($"p{__ATSCPRODGRPID}",      OracleDbType.Varchar2,  eSS.TscProdGrpId,          ParameterDirection.Input)
                    ,new OracleParameter($"p{__AHALTRSNCODE}",       OracleDbType.Varchar2,  eSS.HaltRsnCode,          ParameterDirection.Input)
                    ,new OracleParameter($"p{__APRODUCTID}",         OracleDbType.Varchar2,  eSS.CheckSum,          ParameterDirection.Input)
                    ,new OracleParameter($"p{__ACHECKSUM}",          OracleDbType.Varchar2,  eSS.CheckSum,          ParameterDirection.Input)
                    ,new OracleParameter($"p{__RETURNCODE}",         OracleDbType.Int64,     null,                  ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",         OracleDbType.Varchar2,  500, null,             ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateSecurityInformationNotification(ESecurityInformationNotification eSIN, bool getScriptOnly= false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eSS={_cS6GApp.Common.SerializeObject(eSIN)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateSecurityInformationNotification;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",    OracleDbType.Varchar2,  eSIN.BeginString,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABODYLENGTH}",     OracleDbType.Int64,     eSIN.BodyLength,     ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",        OracleDbType.Varchar2,  eSIN.MsgType,        ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",   OracleDbType.Varchar2,  eSIN.SenderCompID,   ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",   OracleDbType.Varchar2,  eSIN.TargetCompID,   ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",      OracleDbType.Int64,     eSIN.MsgSeqNum,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",    OracleDbType.Varchar2,  eSIN.SendingTime,    ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",       OracleDbType.Varchar2,  eSIN.MarketID,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABOARDID}",        OracleDbType.Varchar2,  eSIN.BoardID,        ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",         OracleDbType.Varchar2,  eSIN.Symbol,         ParameterDirection.Input)
					,new OracleParameter($"p{__ALISTEDSHARES}",   OracleDbType.Int64,     eSIN.ListedShares,   ParameterDirection.Input)
					,new OracleParameter($"p{__AHIGHLIMITPRICE}", OracleDbType.Decimal,   eSIN.HighLimitPrice, ParameterDirection.Input)
					,new OracleParameter($"p{__ALOWLIMITPRICE}",  OracleDbType.Decimal,   eSIN.LowLimitPrice,  ParameterDirection.Input)
					,new OracleParameter($"p{__AREFERENCEPRICE}", OracleDbType.Decimal,   eSIN.ReferencePrice, ParameterDirection.Input)
					,new OracleParameter($"p{__AEVALUATIONPRICE}", OracleDbType.Decimal,  eSIN.EvaluationPrice, ParameterDirection.Input)
					,new OracleParameter($"p{__AHGSTORDERPRICE}", OracleDbType.Decimal,   eSIN.HgstOrderPrice, ParameterDirection.Input)
					,new OracleParameter($"p{__ALWSTORDERPRICE}", OracleDbType.Decimal,   eSIN.LwstOrderPrice, ParameterDirection.Input)
					,new OracleParameter($"p{__AEXCLASSTYPE}",    OracleDbType.Varchar2,  eSIN.ExClassType,    ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",       OracleDbType.Varchar2,  eSIN.CheckSum,       ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",      OracleDbType.Int64,     null,                ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",      OracleDbType.Varchar2,  500, null,           ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};


				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateSymbolClosingInformation(ESymbolClosingInformation eSCI, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eSS={_cS6GApp.Common.SerializeObject(eSCI)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateSymbolClosingInformation;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",           OracleDbType.Varchar2,    eSCI.BeginString,           ParameterDirection.Input)
					,new OracleParameter($"p{__ABODYLENGTH}",            OracleDbType.Int64,       eSCI.BodyLength,            ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",               OracleDbType.Varchar2,    eSCI.MsgType,               ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",          OracleDbType.Varchar2,    eSCI.SenderCompID,          ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",          OracleDbType.Varchar2,    eSCI.TargetCompID,          ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",             OracleDbType.Int64,       eSCI.MsgSeqNum,             ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",           OracleDbType.Varchar2,    eSCI.SendingTime,           ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",              OracleDbType.Varchar2,    eSCI.MarketID,              ParameterDirection.Input)
					,new OracleParameter($"p{__ABOARDID}",               OracleDbType.Varchar2,    eSCI.BoardID,               ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",                OracleDbType.Varchar2,    eSCI.Symbol,                ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOLCLOSEINFOPX}",     OracleDbType.Decimal,     eSCI.SymbolCloseInfoPx,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOLCLOSEINFOPXTYPE}", OracleDbType.Varchar2,    eSCI.SymbolCloseInfoPxType, ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOLCLOSEINFOYIELD}",  OracleDbType.Decimal,     eSCI.SymbolCloseInfoYield,  ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",              OracleDbType.Varchar2,    eSCI.CheckSum,              ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",             OracleDbType.Int64,       null,                       ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",             OracleDbType.Varchar2,    500, null,                  ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateVolatilityInterruption(EVolatilityInterruption eVI, bool getScriptOnly= false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eVI={_cS6GApp.Common.SerializeObject(eVI)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateVolatilityInterruption;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
				     new OracleParameter($"p{__ABEGINSTRING}",              OracleDbType.Varchar2,    eVI.BeginString,              ParameterDirection.Input)
					,new OracleParameter($"p{__ABODYLENGTH}",               OracleDbType.Int64,       eVI.BodyLength,               ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",                  OracleDbType.Varchar2,    eVI.MsgType,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",             OracleDbType.Varchar2,    eVI.SenderCompID,             ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",             OracleDbType.Varchar2,    eVI.TargetCompID,             ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",                OracleDbType.Int64,       eVI.MsgSeqNum,                ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",              OracleDbType.Varchar2,    eVI.SendingTime,              ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",                 OracleDbType.Varchar2,    eVI.MarketID,                 ParameterDirection.Input)
					,new OracleParameter($"p{__ABOARDID}",                  OracleDbType.Varchar2,    eVI.BoardID,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",                   OracleDbType.Varchar2,    eVI.Symbol,                   ParameterDirection.Input)
					,new OracleParameter($"p{__AVITYPECODE}",               OracleDbType.Varchar2,    eVI.VITypeCode,               ParameterDirection.Input)
					,new OracleParameter($"p{__AVIKINDCODE}",               OracleDbType.Varchar2,    eVI.VIKindCode,               ParameterDirection.Input)
					,new OracleParameter($"p{__ASTATICVIBASEPRICE}",        OracleDbType.Decimal,     eVI.StaticVIBasePrice,        ParameterDirection.Input)
					,new OracleParameter($"p{__ADYNAMICVIBASEPRICE}",       OracleDbType.Decimal,     eVI.DynamicVIBasePrice,       ParameterDirection.Input)
					,new OracleParameter($"p{__AVIPRICE}",                  OracleDbType.Decimal,     eVI.VIPrice,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ASTATICVIDISPARTIYRATIO}",   OracleDbType.Decimal,     eVI.StaticVIDispartiyRatio,   ParameterDirection.Input)
					,new OracleParameter($"p{__ADYNAMICVIDISPARTIYRATIO}",  OracleDbType.Decimal,     eVI.DynamicVIDispartiyRatio,  ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",                 OracleDbType.Varchar2,    eVI.CheckSum,                 ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",                OracleDbType.Int64,       null,                         ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",                OracleDbType.Varchar2,    500, null,                    ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};


				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateMarketMakerInformation(EMarketMakerInformation eMMI, bool getScriptOnly= false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eMMI={_cS6GApp.Common.SerializeObject(eMMI)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateMarketMakerInformation;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",              OracleDbType.Varchar2,    eMMI.BeginString,              ParameterDirection.Input)
					,new OracleParameter($"p{__ABODYLENGTH}",               OracleDbType.Int64,       eMMI.BodyLength,               ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",                  OracleDbType.Varchar2,    eMMI.MsgType,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",             OracleDbType.Varchar2,    eMMI.SenderCompID,             ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",             OracleDbType.Varchar2,    eMMI.TargetCompID,             ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",                OracleDbType.Int64,       eMMI.MsgSeqNum,                ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",              OracleDbType.Varchar2,    eMMI.SendingTime,              ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",                 OracleDbType.Varchar2,    eMMI.MarketID,                 ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETMAKERCONTRACTCODE}",  OracleDbType.Varchar2,    eMMI.MarketMakerContractCode,  ParameterDirection.Input)
					,new OracleParameter($"p{__AMEMBERNO}",                 OracleDbType.Varchar2,    eMMI.MemberNo,                 ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",                 OracleDbType.Varchar2,    eMMI.CheckSum,                 ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",                OracleDbType.Int64,       null,                          ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",                OracleDbType.Varchar2,    500, null,                     ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateSymbolEvent(ESymbolEvent eSE , bool getScriptOnly= false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eMMI={_cS6GApp.Common.SerializeObject(eSE)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateSymbolEvent;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
			         new OracleParameter($"p{__ABEGINSTRING}",              OracleDbType.Varchar2,    eSE.BeginString,               ParameterDirection.Input)
					,new OracleParameter($"p{__ABODYLENGTH}",               OracleDbType.Int64,       eSE.BodyLength,                ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",                  OracleDbType.Varchar2,    eSE.MsgType,                   ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",             OracleDbType.Varchar2,    eSE.SenderCompID,              ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",             OracleDbType.Varchar2,    eSE.TargetCompID,              ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",                OracleDbType.Int64,       eSE.MsgSeqNum,                 ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",              OracleDbType.Varchar2,    eSE.SendingTime,               ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",                 OracleDbType.Varchar2,    eSE.MarketID,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",                   OracleDbType.Varchar2,    eSE.Symbol,                    ParameterDirection.Input)
					,new OracleParameter($"p{__AEVENTKINDCODE}",            OracleDbType.Varchar2,    eSE.EventKindCode,             ParameterDirection.Input)
					,new OracleParameter($"p{__AEVENTOCCURRENCEREASONCODE}",OracleDbType.Varchar2,    eSE.EventOccurrenceReasonCode, ParameterDirection.Input)
					,new OracleParameter($"p{__AEVENTSTARTDATE}",           OracleDbType.Date,        eSE.EventStartDate,            ParameterDirection.Input)
					,new OracleParameter($"p{__AEVENTENDDATE}",             OracleDbType.Date,        eSE.EventEndDate,              ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",                 OracleDbType.Varchar2,    eSE.CheckSum,                  ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",                OracleDbType.Int64,       null,                          ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",                OracleDbType.Varchar2,    500, null,                     ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};


				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateIndexConstituentsInformation(EIndexConstituentsInformation eICI, bool getScriptOnly= false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eMMI={_cS6GApp.Common.SerializeObject(eICI)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateIndexConstituentsInformation;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
			         new OracleParameter($"p{__ABEGINSTRING}",      OracleDbType.Varchar2,    eICI.BeginString,      ParameterDirection.Input)
					,new OracleParameter($"p{__ABODYLENGTH}",       OracleDbType.Int64,       eICI.BodyLength,       ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",          OracleDbType.Varchar2,    eICI.MsgType,          ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",     OracleDbType.Varchar2,    eICI.SenderCompID,     ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",     OracleDbType.Varchar2,    eICI.TargetCompID,     ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",        OracleDbType.Int64,       eICI.MsgSeqNum,        ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",      OracleDbType.Varchar2,    eICI.SendingTime,      ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",         OracleDbType.Varchar2,    eICI.MarketID,         ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETINDEXCLASS}", OracleDbType.Varchar2,    eICI.MarketIndexClass, ParameterDirection.Input)
					,new OracleParameter($"p{__AINDEXSTYPECODE}",   OracleDbType.Varchar2,    eICI.IndexsTypeCode,   ParameterDirection.Input)
					,new OracleParameter($"p{__ACURRENCY}",         OracleDbType.Varchar2,    eICI.Currency,         ParameterDirection.Input)
					,new OracleParameter($"p{__AIDXNAME}",          OracleDbType.Varchar2,    eICI.IdxName,          ParameterDirection.Input)
					,new OracleParameter($"p{__AIDXENGLISHNAME}",   OracleDbType.Varchar2,    eICI.IdxEnglishName,   ParameterDirection.Input)
					,new OracleParameter($"p{__ATOTALMSGNO}",       OracleDbType.Int64,       eICI.TotalMsgNo,       ParameterDirection.Input)
					,new OracleParameter($"p{__ACURRENTMSGNO}",     OracleDbType.Int64,       eICI.CurrentMsgNo,     ParameterDirection.Input)
                    ,new OracleParameter($"p{__ASYMBOL}",           OracleDbType.Varchar2,    eICI.Symbol,           ParameterDirection.Input)
                    ,new OracleParameter($"p{__ACHECKSUM}",         OracleDbType.Varchar2,    eICI.CheckSum,         ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",        OracleDbType.Int64,       null,                  ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",        OracleDbType.Varchar2,    500, null,             ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateRandomEnd(ERandomEnd eRE, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eMMI={_cS6GApp.Common.SerializeObject(eRE)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateRandomEnd;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",                                 OracleDbType.Varchar2,    eRE.BeginString,                                    ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",                                  OracleDbType.Int64,       eRE.BodyLength,                                     ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",                                     OracleDbType.Varchar2,    eRE.MsgType,                                        ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",                                OracleDbType.Varchar2,    eRE.SenderCompID,                                   ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",                                OracleDbType.Varchar2,    eRE.TargetCompID,                                   ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",                                   OracleDbType.Int64,       eRE.MsgSeqNum,                                      ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",                                 OracleDbType.Varchar2,    TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(eRE.SendingTime, "yyyy-MM-dd HH:mm:ss.fff", null), timeZone).ToString("yyyy-MM-dd HH:mm:ss.fff"),                                    ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",                                    OracleDbType.Varchar2,    eRE.MarketID,                                       ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",                                      OracleDbType.Varchar2,    eRE.Symbol,                                         ParameterDirection.Input)
					,new OracleParameter($"p{__ABOARDID}",                                     OracleDbType.Varchar2,    eRE.BoardID,                                        ParameterDirection.Input)
					,new OracleParameter($"p{__ATRANSACTTIME}",                                OracleDbType.Varchar2,    eRE.TransactTime,                                   ParameterDirection.Input)
					,new OracleParameter($"p{__RANDOMENDAPPLYCLASSIFICATION}",                 OracleDbType.Varchar2,    eRE.RandomEndApplyClassification,                   ParameterDirection.Input)
					,new OracleParameter($"p{__RANDOMENDTENTATIVEEXECUTIONPRICE}",             OracleDbType.Decimal,     eRE.RandomEndTentativeExecutionPrice,               ParameterDirection.Input)
					,new OracleParameter($"p{__RANDOMENDESTIMATEDHIGHESTPRICE}",               OracleDbType.Decimal,     eRE.RandomEndEstimatedHighestPrice,                 ParameterDirection.Input)
					,new OracleParameter($"p{__RANDOMENDESTIMATEDHIGHESTPRICEDISPARATERATIO}", OracleDbType.Decimal,     eRE.RandomEndEstimatedHighestPriceDisparateRatio,   ParameterDirection.Input)
					,new OracleParameter($"p{__RANDOMENDESTIMATEDLOWESTPRICE}",                OracleDbType.Decimal,     eRE.RandomEndEstimatedLowestPrice,                  ParameterDirection.Input)
					,new OracleParameter($"p{__RANDOMENDESTIMATEDLOWESTPRICEDISPARATERATIO}",  OracleDbType.Decimal,     eRE.RandomEndEstimatedLowestPriceDisparateRatio,    ParameterDirection.Input)
					,new OracleParameter($"p{__ALATESTPRICE}",                                 OracleDbType.Decimal,     eRE.LatestPrice,                                    ParameterDirection.Input)
					,new OracleParameter($"p{__ALATESTPRICEDISPARATERATIO}",                   OracleDbType.Decimal,     eRE.LatestPriceDisparateRatio,                      ParameterDirection.Input)
					,new OracleParameter($"p{__ARANDOMENDRELEASETIME}",                        OracleDbType.Varchar2,    eRE.RandomEndReleaseTimes,                          ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",                                    OracleDbType.Varchar2,    eRE.CheckSum,                                       ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",                                   OracleDbType.Int64,       null,                                               ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",                                   OracleDbType.Varchar2,    500, null,                                          ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// 4.10 price
		/// insert data vao table tRandomEnd
		/// chi insert 1 row 1 lan exec sp
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdatePriceAll(EPrice eP, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eP={_cS6GApp.Common.SerializeObject(eP)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdatePriceAll;
				EDalResult result = null;

                // 1. input            
                OracleParameter[] paramArrary = new OracleParameter[]
                {
                    new OracleParameter($"p{__ABEGINSTRING}",        OracleDbType.Varchar2,    eP.BeginString,        ParameterDirection.Input),
                    new OracleParameter($"p{__ABODYLENGTH}",         OracleDbType.Int64,       eP.BodyLength,         ParameterDirection.Input),
                    new OracleParameter($"p{__AMSGTYPE}",            OracleDbType.Varchar2,    eP.MsgType,            ParameterDirection.Input),
                    new OracleParameter($"p{__ASENDERCOMPID}",       OracleDbType.Varchar2,    eP.SenderCompID,       ParameterDirection.Input),
                    new OracleParameter($"p{__ATARGETCOMPID}",       OracleDbType.Varchar2,    eP.TargetCompID,       ParameterDirection.Input),
                    new OracleParameter($"p{__AMSGSEQNUM}",          OracleDbType.Int64,       eP.MsgSeqNum,          ParameterDirection.Input),
                    new OracleParameter($"p{__ASENDINGTIME}",        OracleDbType.Varchar2,    eP.SendingTime,        ParameterDirection.Input),
                    new OracleParameter($"p{__AMARKETID}",           OracleDbType.Varchar2,    eP.MarketID,           ParameterDirection.Input),
                    new OracleParameter($"p{__ABOARDID}",            OracleDbType.Varchar2,    eP.BoardID,            ParameterDirection.Input),
                    new OracleParameter($"p{__ATRADINGSESSIONID}",   OracleDbType.Varchar2,    eP.TradingSessionID,   ParameterDirection.Input),
                    new OracleParameter($"p{__ASYMBOL}",             OracleDbType.Varchar2,    eP.Symbol,             ParameterDirection.Input),
                    new OracleParameter($"p{__ATRADEDATE}",          OracleDbType.Varchar2,    eP.TradeDate,          ParameterDirection.Input),
                    new OracleParameter($"p{__ATRANSACTTIME}",       OracleDbType.Varchar2,    eP.TransactTime,       ParameterDirection.Input),

                    eP.TotalVolumeTraded != -9999999 ? new OracleParameter($"p{__ATOTALVOLUMETRADED}", OracleDbType.Int64, eP.TotalVolumeTraded, ParameterDirection.Input) : new OracleParameter($"p{__ATOTALVOLUMETRADED}", OracleDbType.Int64, null, ParameterDirection.Input),
                    new OracleParameter($"p{__AGROSSTRADEAMT}", OracleDbType.Decimal, eP.GrossTradeAmt, ParameterDirection.Input),

                    eP.SellTotOrderQty != -9999999 ? new OracleParameter($"p{__ASELLTOTORDERQTY}", OracleDbType.Int64, eP.SellTotOrderQty, ParameterDirection.Input) : new OracleParameter($"p{__ASELLTOTORDERQTY}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyTotOrderQty != -9999999 ? new OracleParameter($"p{__ABUYTOTORDERQTY}", OracleDbType.Int64, eP.BuyTotOrderQty, ParameterDirection.Input) : new OracleParameter($"p{__ABUYTOTORDERQTY}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellValidOrderCnt != -9999999 ? new OracleParameter($"p{__ASELLVALIDORDERCNT}", OracleDbType.Int64, eP.SellValidOrderCnt, ParameterDirection.Input) : new OracleParameter($"p{__ASELLVALIDORDERCNT}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyTotOrderQty != -9999999 ? new OracleParameter($"p{__ABUYVALIDORDERCNT}", OracleDbType.Int64, eP.BuyValidOrderCnt, ParameterDirection.Input) : new OracleParameter($"p{__ABUYVALIDORDERCNT}", OracleDbType.Int64, null, ParameterDirection.Input),

                    new OracleParameter($"p{__ANOMDENTRIES}", OracleDbType.Decimal, eP.NoMDEntries, ParameterDirection.Input),

					//TOP 1
					eP.BuyPrice1 != -9999999 ? new OracleParameter($"p{__ABUYPRICE1}", OracleDbType.Decimal, eP.BuyPrice1, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE1}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice1 != -9999999 ? new OracleParameter($"p{__ABUYQUANTITY1}", OracleDbType.Int64, eP.BuyQuantity1, ParameterDirection.Input) : new OracleParameter($"p{__ABUYQUANTITY1}", OracleDbType.Int64, null, ParameterDirection.Input) ,
                    eP.BuyPrice1 != -9999999 ? new OracleParameter($"p{__ABUYPRICE1_NOO}", OracleDbType.Int64, eP.BuyPrice1_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE1_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice1 != -9999999 ? new OracleParameter($"p{__ABUYPRICE1_MDEY}", OracleDbType.Decimal, eP.BuyPrice1_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE1_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice1 != -9999999 ? new OracleParameter($"p{__ABUYPRICE1_MDEMMS}", OracleDbType.Int64, eP.BuyPrice1_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE1_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

                    eP.SellPrice1 != -9999999 ? new OracleParameter($"p{__ASELLPRICE1}", OracleDbType.Decimal, eP.SellPrice1, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE1}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice1 != -9999999 ? new OracleParameter($"p{__ASELLQUANTITY1}", OracleDbType.Int64, eP.SellQuantity1, ParameterDirection.Input): new OracleParameter($"p{__ASELLQUANTITY1}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice1 != -9999999 ? new OracleParameter($"p{__ASELLPRICE1_NOO}", OracleDbType.Int64, eP.SellPrice1_NOO, ParameterDirection.Input): new OracleParameter($"p{__ASELLPRICE1_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice1 != -9999999 ? new OracleParameter($"p{__ASELLPRICE1_MDEY}", OracleDbType.Decimal, eP.SellPrice1_MDEY, ParameterDirection.Input): new OracleParameter($"p{__ASELLPRICE1_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice1 != -9999999 ? new OracleParameter($"p{__ASELLPRICE1_MDEMMS}", OracleDbType.Int64, eP.SellPrice1_MDEMMS, ParameterDirection.Input): new OracleParameter($"p{__ASELLPRICE1_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

					//TOP 2
					eP.BuyPrice2 != -9999999 ? new OracleParameter($"p{__ABUYPRICE2}", OracleDbType.Decimal, eP.BuyPrice2, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE2}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice2 != -9999999 ? new OracleParameter($"p{__ABUYQUANTITY2}", OracleDbType.Int64, eP.BuyQuantity2, ParameterDirection.Input) : new OracleParameter($"p{__ABUYQUANTITY2}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice2 != -9999999 ? new OracleParameter($"p{__ABUYPRICE2_NOO}", OracleDbType.Int64, eP.BuyPrice2_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE2_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice2 != -9999999 ? new OracleParameter($"p{__ABUYPRICE2_MDEY}", OracleDbType.Decimal, eP.BuyPrice2_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE2_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice2 != -9999999 ? new OracleParameter($"p{__ABUYPRICE2_MDEMMS}", OracleDbType.Int64, eP.BuyPrice2_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE2_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

                    eP.SellPrice2 != -9999999 ? new OracleParameter($"p{__ASELLPRICE2}", OracleDbType.Decimal, eP.SellPrice2, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE2}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice2 != -9999999 ? new OracleParameter($"p{__ASELLQUANTITY2}", OracleDbType.Int64, eP.SellQuantity2, ParameterDirection.Input): new OracleParameter($"p{__ASELLQUANTITY2}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice2 != -9999999 ? new OracleParameter($"p{__ASELLPRICE2_NOO}", OracleDbType.Int64, eP.SellPrice2_NOO, ParameterDirection.Input): new OracleParameter($"p{__ASELLPRICE2_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice2 != -9999999 ? new OracleParameter($"p{__ASELLPRICE2_MDEY}", OracleDbType.Decimal, eP.SellPrice2_MDEY, ParameterDirection.Input): new OracleParameter($"p{__ASELLPRICE2_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice2 != -9999999 ? new OracleParameter($"p{__ASELLPRICE2_MDEMMS}", OracleDbType.Int64, eP.SellPrice2_MDEMMS, ParameterDirection.Input): new OracleParameter($"p{__ASELLPRICE2_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

					//TOP 3
					eP.BuyPrice3 != -9999999 ? new OracleParameter($"p{__ABUYPRICE3}", OracleDbType.Decimal, eP.BuyPrice3, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE3}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice3 != -9999999 ? new OracleParameter($"p{__ABUYQUANTITY3}", OracleDbType.Int64, eP.BuyQuantity3, ParameterDirection.Input) : new OracleParameter($"p{__ABUYQUANTITY3}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice3 != -9999999 ? new OracleParameter($"p{__ABUYPRICE3_NOO}", OracleDbType.Int64, eP.BuyPrice3_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE3_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice3 != -9999999 ? new OracleParameter($"p{__ABUYPRICE3_MDEY}", OracleDbType.Decimal, eP.BuyPrice3_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE3_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice3 != -9999999 ? new OracleParameter($"p{__ABUYPRICE3_MDEMMS}", OracleDbType.Int64, eP.BuyPrice3_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE3_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

                    eP.SellPrice3 != -9999999 ? new OracleParameter($"p{__ASELLPRICE3}", OracleDbType.Decimal, eP.SellPrice3, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE3}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice3 != -9999999 ? new OracleParameter($"p{__ASELLQUANTITY3}", OracleDbType.Int64, eP.SellQuantity3, ParameterDirection.Input): new OracleParameter($"p{__ASELLQUANTITY3}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice3 != -9999999 ? new OracleParameter($"p{__ASELLPRICE3_NOO}", OracleDbType.Int64, eP.SellPrice3_NOO, ParameterDirection.Input): new OracleParameter($"p{__ASELLPRICE3_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice3 != -9999999 ? new OracleParameter($"p{__ASELLPRICE3_MDEY}", OracleDbType.Decimal, eP.SellPrice3_MDEY, ParameterDirection.Input): new OracleParameter($"p{__ASELLPRICE3_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice3 != -9999999 ? new OracleParameter($"p{__ASELLPRICE3_MDEMMS}", OracleDbType.Int64, eP.SellPrice3_MDEMMS, ParameterDirection.Input): new OracleParameter($"p{__ASELLPRICE3_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

					//TOP 4
					 eP.BuyPrice4 != -9999999 ? new OracleParameter($"p{__ABUYPRICE4}", OracleDbType.Decimal, eP.BuyPrice4, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE4}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice4 != -9999999 ? new OracleParameter($"p{__ABUYQUANTITY4}", OracleDbType.Int64, eP.BuyQuantity4, ParameterDirection.Input) : new OracleParameter($"p{__ABUYQUANTITY4}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice4 != -9999999 ? new OracleParameter($"p{__ABUYPRICE4_NOO}", OracleDbType.Int64, eP.BuyPrice4_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE4_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice4 != -9999999 ? new OracleParameter($"p{__ABUYPRICE4_MDEY}", OracleDbType.Decimal, eP.BuyPrice4_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE4_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice4 != -9999999 ? new OracleParameter($"p{__ABUYPRICE4_MDEMMS}", OracleDbType.Int64, eP.BuyPrice4_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE4_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

                    eP.SellPrice4 != -9999999 ? new OracleParameter($"p{__ASELLPRICE4}", OracleDbType.Decimal, eP.SellPrice4, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE4}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice4 != -9999999 ? new OracleParameter($"p{__ASELLQUANTITY4}", OracleDbType.Int64, eP.SellQuantity4, ParameterDirection.Input): new OracleParameter($"p{__ASELLQUANTITY4}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice4 != -9999999 ? new OracleParameter($"p{__ASELLPRICE4_NOO}", OracleDbType.Int64, eP.SellPrice4_NOO, ParameterDirection.Input): new OracleParameter($"p{__ASELLPRICE4_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice4 != -9999999 ? new OracleParameter($"p{__ASELLPRICE4_MDEY}", OracleDbType.Decimal, eP.SellPrice4_MDEY, ParameterDirection.Input): new OracleParameter($"p{__ASELLPRICE4_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice4 != -9999999 ? new OracleParameter($"p{__ASELLPRICE4_MDEMMS}", OracleDbType.Int64, eP.SellPrice4_MDEMMS, ParameterDirection.Input): new OracleParameter($"p{__ASELLPRICE4_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

					//TOP 5
					eP.BuyPrice5 != -9999999 ? new OracleParameter($"p{__ABUYPRICE5}", OracleDbType.Decimal, eP.BuyPrice5, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE5}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice5 != -9999999 ? new OracleParameter($"p{__ABUYQUANTITY5}", OracleDbType.Int64, eP.BuyQuantity5, ParameterDirection.Input) : new OracleParameter($"p{__ABUYQUANTITY5}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice5 != -9999999 ? new OracleParameter($"p{__ABUYPRICE5_NOO}", OracleDbType.Int64, eP.BuyPrice5_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE5_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice5 != -9999999 ? new OracleParameter($"p{__ABUYPRICE5_MDEY}", OracleDbType.Decimal, eP.BuyPrice5_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE5_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice5 != -9999999 ? new OracleParameter($"p{__ABUYPRICE5_MDEMMS}", OracleDbType.Int64, eP.BuyPrice5_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE5_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

                    eP.SellPrice5 != -9999999 ? new OracleParameter($"p{__ASELLPRICE5}", OracleDbType.Decimal, eP.SellPrice5, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE5}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice5 != -9999999 ? new OracleParameter($"p{__ASELLQUANTITY5}", OracleDbType.Int64, eP.SellQuantity5, ParameterDirection.Input) : new OracleParameter($"p{__ASELLQUANTITY5}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice5 != -9999999 ? new OracleParameter($"p{__ASELLPRICE5_NOO}", OracleDbType.Int64, eP.SellPrice5_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE5_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice5 != -9999999 ? new OracleParameter($"p{__ASELLPRICE5_MDEY}", OracleDbType.Decimal, eP.SellPrice5_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE5_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice5 != -9999999 ? new OracleParameter($"p{__ASELLPRICE5_MDEMMS}", OracleDbType.Int64, eP.SellPrice5_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE5_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),
                        
					//TOP 6
					eP.BuyPrice6 != -9999999 ? new OracleParameter($"p{__ABUYPRICE6}", OracleDbType.Decimal, eP.BuyPrice6, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE6}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice6 != -9999999 ? new OracleParameter($"p{__ABUYQUANTITY6}", OracleDbType.Int64, eP.BuyQuantity6, ParameterDirection.Input) : new OracleParameter($"p{__ABUYQUANTITY6}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice6 != -9999999 ? new OracleParameter($"p{__ABUYPRICE6_NOO}", OracleDbType.Int64, eP.BuyPrice6_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE6_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice6 != -9999999 ? new OracleParameter($"p{__ABUYPRICE6_MDEY}", OracleDbType.Decimal, eP.BuyPrice6_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE6_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice6 != -9999999 ? new OracleParameter($"p{__ABUYPRICE6_MDEMMS}", OracleDbType.Int64, eP.BuyPrice6_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE6_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

                    eP.SellPrice6 != -9999999 ? new OracleParameter($"p{__ASELLPRICE6}", OracleDbType.Decimal, eP.SellPrice6, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE6}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice6 != -9999999 ? new OracleParameter($"p{__ASELLQUANTITY6}", OracleDbType.Int64, eP.SellQuantity6, ParameterDirection.Input) : new OracleParameter($"p{__ASELLQUANTITY6}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice6 != -9999999 ? new OracleParameter($"p{__ASELLPRICE6_NOO}", OracleDbType.Int64, eP.SellPrice6_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE6_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice6 != -9999999 ? new OracleParameter($"p{__ASELLPRICE6_MDEY}", OracleDbType.Decimal, eP.SellPrice6_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE6_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice6 != -9999999 ? new OracleParameter($"p{__ASELLPRICE6_MDEMMS}", OracleDbType.Int64, eP.SellPrice6_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE6_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

					//TOP 7
					 eP.BuyPrice7 != -9999999 ? new OracleParameter($"p{__ABUYPRICE7}", OracleDbType.Decimal, eP.BuyPrice7, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE7}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice7 != -9999999 ? new OracleParameter($"p{__ABUYQUANTITY7}", OracleDbType.Int64, eP.BuyQuantity7, ParameterDirection.Input) : new OracleParameter($"p{__ABUYQUANTITY7}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice7 != -9999999 ? new OracleParameter($"p{__ABUYPRICE7_NOO}", OracleDbType.Int64, eP.BuyPrice7_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE7_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice7 != -9999999 ? new OracleParameter($"p{__ABUYPRICE7_MDEY}", OracleDbType.Decimal, eP.BuyPrice7_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE7_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice7 != -9999999 ? new OracleParameter($"p{__ABUYPRICE7_MDEMMS}", OracleDbType.Int64, eP.BuyPrice7_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE7_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

                    eP.SellPrice7 != -9999999 ? new OracleParameter($"p{__ASELLPRICE7}", OracleDbType.Decimal, eP.SellPrice7, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE7}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice7 != -9999999 ? new OracleParameter($"p{__ASELLQUANTITY7}", OracleDbType.Int64, eP.SellQuantity7, ParameterDirection.Input) : new OracleParameter($"p{__ASELLQUANTITY7}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice7 != -9999999 ? new OracleParameter($"p{__ASELLPRICE7_NOO}", OracleDbType.Int64, eP.SellPrice7_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE7_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice7 != -9999999 ? new OracleParameter($"p{__ASELLPRICE7_MDEY}", OracleDbType.Decimal, eP.SellPrice7_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE7_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice7 != -9999999 ? new OracleParameter($"p{__ASELLPRICE7_MDEMMS}", OracleDbType.Int64, eP.SellPrice7_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE7_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

					//TOP 8
					eP.BuyPrice8 != -9999999 ? new OracleParameter($"p{__ABUYPRICE8}", OracleDbType.Decimal, eP.BuyPrice8, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE8}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice8 != -9999999 ? new OracleParameter($"p{__ABUYQUANTITY8}", OracleDbType.Int64, eP.BuyQuantity8, ParameterDirection.Input) : new OracleParameter($"p{__ABUYQUANTITY8}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice8 != -9999999 ? new OracleParameter($"p{__ABUYPRICE8_NOO}", OracleDbType.Int64, eP.BuyPrice8_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE8_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice8 != -9999999 ? new OracleParameter($"p{__ABUYPRICE8_MDEY}", OracleDbType.Decimal, eP.BuyPrice8_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE8_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice8 != -9999999 ? new OracleParameter($"p{__ABUYPRICE8_MDEMMS}", OracleDbType.Int64, eP.BuyPrice8_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE8_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

                    eP.SellPrice8 != -9999999 ? new OracleParameter($"p{__ASELLPRICE8}", OracleDbType.Decimal, eP.SellPrice8, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE8}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice8 != -9999999 ? new OracleParameter($"p{__ASELLQUANTITY8}", OracleDbType.Int64, eP.SellQuantity8, ParameterDirection.Input) : new OracleParameter($"p{__ASELLQUANTITY8}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice8 != -9999999 ? new OracleParameter($"p{__ASELLPRICE8_NOO}", OracleDbType.Int64, eP.SellPrice8_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE8_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice8 != -9999999 ? new OracleParameter($"p{__ASELLPRICE8_MDEY}", OracleDbType.Decimal, eP.SellPrice8_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE8_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice8 != -9999999 ? new OracleParameter($"p{__ASELLPRICE8_MDEMMS}", OracleDbType.Int64, eP.SellPrice8_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE8_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

					//TOP 9
					eP.BuyPrice9 != -9999999 ? new OracleParameter($"p{__ABUYPRICE9}", OracleDbType.Decimal, eP.BuyPrice9, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE9}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice9 != -9999999 ? new OracleParameter($"p{__ABUYQUANTITY9}", OracleDbType.Int64, eP.BuyQuantity9, ParameterDirection.Input) : new OracleParameter($"p{__ABUYQUANTITY9}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice9 != -9999999 ? new OracleParameter($"p{__ABUYPRICE9_NOO}", OracleDbType.Int64, eP.BuyPrice9_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE9_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice9 != -9999999 ? new OracleParameter($"p{__ABUYPRICE9_MDEY}", OracleDbType.Decimal, eP.BuyPrice9_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE9_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice9 != -9999999 ? new OracleParameter($"p{__ABUYPRICE9_MDEMMS}", OracleDbType.Int64, eP.BuyPrice9_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE9_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

                    eP.SellPrice9 != -9999999 ? new OracleParameter($"p{__ASELLPRICE9}", OracleDbType.Decimal, eP.SellPrice9, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE9}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice9 != -9999999 ? new OracleParameter($"p{__ASELLQUANTITY9}", OracleDbType.Int64, eP.SellQuantity9, ParameterDirection.Input) : new OracleParameter($"p{__ASELLQUANTITY9}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice9 != -9999999 ? new OracleParameter($"p{__ASELLPRICE9_NOO}", OracleDbType.Int64, eP.SellPrice9_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE9_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice9 != -9999999 ? new OracleParameter($"p{__ASELLPRICE9_MDEY}", OracleDbType.Decimal, eP.SellPrice9_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE9_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice9 != -9999999 ? new OracleParameter($"p{__ASELLPRICE9_MDEMMS}", OracleDbType.Int64, eP.SellPrice9_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE9_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

					//TOP 10
					eP.BuyPrice10 != -9999999 ? new OracleParameter($"p{__ABUYPRICE10}", OracleDbType.Decimal, eP.BuyPrice10, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE10}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice10 != -9999999 ? new OracleParameter($"p{__ABUYQUANTITY10}", OracleDbType.Int64, eP.BuyQuantity10, ParameterDirection.Input) : new OracleParameter($"p{__ABUYQUANTITY10}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice10 != -9999999 ? new OracleParameter($"p{__ABUYPRICE10_NOO}", OracleDbType.Int64, eP.BuyPrice10_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE10_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.BuyPrice10 != -9999999 ? new OracleParameter($"p{__ABUYPRICE10_MDEY}", OracleDbType.Decimal, eP.BuyPrice10_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE10_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.BuyPrice10 != -9999999 ? new OracleParameter($"p{__ABUYPRICE10_MDEMMS}", OracleDbType.Int64, eP.BuyPrice10_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ABUYPRICE10_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

                    eP.SellPrice10 != -9999999 ? new OracleParameter($"p{__ASELLPRICE10}", OracleDbType.Decimal, eP.SellPrice10, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE10}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice10 != -9999999 ? new OracleParameter($"p{__ASELLQUANTITY10}", OracleDbType.Int64, eP.SellQuantity10, ParameterDirection.Input) : new OracleParameter($"p{__ASELLQUANTITY10}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice10 != -9999999 ? new OracleParameter($"p{__ASELLPRICE10_NOO}", OracleDbType.Int64, eP.SellPrice10_NOO, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE10_NOO}", OracleDbType.Int64, null, ParameterDirection.Input),
                    eP.SellPrice10 != -9999999 ? new OracleParameter($"p{__ASELLPRICE10_MDEY}", OracleDbType.Decimal, eP.SellPrice10_MDEY, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE10_MDEY}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.SellPrice10 != -9999999 ? new OracleParameter($"p{__ASELLPRICE10_MDEMMS}", OracleDbType.Int64, eP.SellPrice10_MDEMMS, ParameterDirection.Input) : new OracleParameter($"p{__ASELLPRICE10_MDEMMS}", OracleDbType.Int64, null, ParameterDirection.Input),

                    eP.MatchPrice != -9999999 ? new OracleParameter($"p{__AMATCHPRICE}", OracleDbType.Decimal, eP.MatchPrice, ParameterDirection.Input) : new OracleParameter($"p{__AMATCHPRICE}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.MatchPrice != -9999999 ? new OracleParameter($"p{__AMATCHQUANTITY}", OracleDbType.Int64, eP.MatchQuantity, ParameterDirection.Input): new OracleParameter($"p{__AMATCHQUANTITY}", OracleDbType.Int64, null, ParameterDirection.Input),

                    eP.OpenPrice != -9999999 ? new OracleParameter($"p{__AOPENPRICE}", OracleDbType.Decimal, eP.OpenPrice, ParameterDirection.Input) : new OracleParameter($"p{__AOPENPRICE}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.ClosePrice != -9999999 ? new OracleParameter($"p{__ACLOSEPRICE}", OracleDbType.Decimal, eP.ClosePrice, ParameterDirection.Input) : new OracleParameter($"p{__ACLOSEPRICE}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.HighestPrice != -9999999 ? new OracleParameter($"p{__AHIGHESTPRICE}", OracleDbType.Decimal, eP.HighestPrice, ParameterDirection.Input): new OracleParameter($"p{__AHIGHESTPRICE}", OracleDbType.Decimal, null, ParameterDirection.Input),
                    eP.LowestPrice != -9999999 ? new OracleParameter($"p{__ALOWESTPRICE}", OracleDbType.Decimal, eP.LowestPrice, ParameterDirection.Input): new OracleParameter($"p{__ALOWESTPRICE}", OracleDbType.Decimal, null, ParameterDirection.Input),

                    new OracleParameter($"p{__ACHECKSUM}", OracleDbType.Varchar2, eP.CheckSum, ParameterDirection.Input),

                   // new OracleParameter($"p{__REPEATINGDATAFIX}", OracleDbType.Varchar2, eP.RepeatingDataFix, ParameterDirection.Input),
					//new OracleParameter($"p{__REPEATINGDATAJSON}", OracleDbType.Varchar2, eP.RepeatingDataJson, ParameterDirection.Input),
					
					new OracleParameter($"p{__RETURNCODE}", OracleDbType.Int64, null, ParameterDirection.Output),
                    new OracleParameter($"p{__RETURNMESS}", OracleDbType.Varchar2, 500, null, ParameterDirection.Output), // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
					
                };
                // ko exec sp, chi lay script de run bulk update sau nay 
                if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateInvestorperIndustry(EInvestorPerIndustry eIPI, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eIPI={_cS6GApp.Common.SerializeObject(eIPI)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateInvestorperIndustry;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
				     new OracleParameter($"p{__ABEGINSTRING}",            OracleDbType.Varchar2,  eIPI.BeginString,             ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",             OracleDbType.Int64,     eIPI.BodyLength,              ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",                OracleDbType.Varchar2,  eIPI.MsgType,                 ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",           OracleDbType.Varchar2,  eIPI.SenderCompID,            ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",           OracleDbType.Varchar2,  eIPI.TargetCompID,            ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",              OracleDbType.Int64,     eIPI.MsgSeqNum,               ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",            OracleDbType.Varchar2,  eIPI.SendingTime,             ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",               OracleDbType.Varchar2,  eIPI.MarketID,                ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETINDEXCLASS}",       OracleDbType.Varchar2,  eIPI.MarketIndexClass,        ParameterDirection.Input)
					,new OracleParameter($"p{__ATRANSACTTIME}",           OracleDbType.Varchar2,  eIPI.TransactTime,            ParameterDirection.Input)
					,new OracleParameter($"p{__AINDEXSTYPECODE}",         OracleDbType.Varchar2,  eIPI.IndexsTypeCode,          ParameterDirection.Input)
					,new OracleParameter($"p{__ACURRENCY}",               OracleDbType.Varchar2,  eIPI.Currency,                ParameterDirection.Input)
					,new OracleParameter($"p{__ABONDCLASSIFICATIONCODE}", OracleDbType.Varchar2,  eIPI.BondClassificationCode,  ParameterDirection.Input)
					,new OracleParameter($"p{__ASECURITYGROUPID }",       OracleDbType.Varchar2,  eIPI.SecurityGroupID,         ParameterDirection.Input)
					,new OracleParameter($"p{__AINVESTCODE}",             OracleDbType.Varchar2,  eIPI.InvestCode,              ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLVOLUME}",             OracleDbType.Int64,     eIPI.SellVolume,              ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLTRADEAMOUNT}",        OracleDbType.Decimal,   eIPI.SellTradeAmount,         ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYVOLUME}",              OracleDbType.Int64,     eIPI.BuyVolume,               ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYTRADEDAMOUNT}",        OracleDbType.Decimal,   eIPI.BuyTradedAmount,         ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",               OracleDbType.Varchar2,  eIPI.CheckSum,                ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",              OracleDbType.Int64,     null,                         ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",              OracleDbType.Varchar2,  500, null,                    ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateInvestorperSymbol(EInvestorPerSymbol eIPS, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eIPS={this._cS6GApp.Common.SerializeObject(eIPS)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateInvestorperSymbol;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",      OracleDbType.Varchar2,  eIPS.BeginString,             ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",       OracleDbType.Int64,     eIPS.BodyLength,              ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",          OracleDbType.Varchar2,  eIPS.MsgType,                 ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",     OracleDbType.Varchar2,  eIPS.SenderCompID,            ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",     OracleDbType.Varchar2,  eIPS.TargetCompID,            ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",        OracleDbType.Int64,     eIPS.MsgSeqNum,               ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",      OracleDbType.Varchar2,  eIPS.SendingTime,             ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",         OracleDbType.Varchar2,  eIPS.MarketID,                ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",           OracleDbType.Varchar2,  eIPS.Symbol,                  ParameterDirection.Input)
					,new OracleParameter($"p{__AINVESTCODE}",       OracleDbType.Varchar2,  eIPS.InvestCode,              ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLVOLUME}",       OracleDbType.Int64,     eIPS.SellVolume,              ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLTRADEAMOUNT}",  OracleDbType.Decimal,   eIPS.SellTradeAmount,         ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYVOLUME}",        OracleDbType.Int64,     eIPS.BuyVolume,               ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYTRADEDAMOUNT}",  OracleDbType.Decimal,   eIPS.BuyTradedAmount,         ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",         OracleDbType.Varchar2,  eIPS.CheckSum,                ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",        OracleDbType.Int64,     null,                          ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",        OracleDbType.Varchar2,  500, null,                     ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
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
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateIndex;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",                      OracleDbType.Varchar2,  eI.BeginString,                      ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",                       OracleDbType.Int64,     eI.BodyLength,                       ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",                          OracleDbType.Varchar2,  eI.MsgType,                          ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",                     OracleDbType.Varchar2,  eI.SenderCompID,                     ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",                     OracleDbType.Varchar2,  eI.TargetCompID,                     ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",                        OracleDbType.Int64,     eI.MsgSeqNum,                        ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",                      OracleDbType.Varchar2,  eI.SendingTime,                      ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",                         OracleDbType.Varchar2,  eI.MarketID,                         ParameterDirection.Input)
                    ,new OracleParameter($"p{__ATRADINGSESSIONID}",                 OracleDbType.Varchar2,  eI.TradingSessionID,                 ParameterDirection.Input)
                    ,new OracleParameter($"p{__AMARKETINDEXCLASS}",                 OracleDbType.Varchar2,  eI.MarketIndexClass,                 ParameterDirection.Input)			
					,new OracleParameter($"p{__AINDEXSTYPECODE}",                   OracleDbType.Varchar2,  eI.IndexsTypeCode,                   ParameterDirection.Input)
					,new OracleParameter($"p{__ACURRENCY}",                         OracleDbType.Varchar2,  eI.Currency,                         ParameterDirection.Input)
					,new OracleParameter($"p{__ATRANSACTTIME}",                     OracleDbType.Varchar2,  eI.TransactTime,                     ParameterDirection.Input)
					,new OracleParameter($"p{__ATRANSDATE}",                        OracleDbType.Varchar2,  eI.TransDate,                        ParameterDirection.Input)
					,new OracleParameter($"p{__AVALUEINDEXES}",                     OracleDbType.Decimal,   eI.ValueIndexes,                     ParameterDirection.Input)
                    ,new OracleParameter($"p{__ATOTALVOLUMETRADED}",                OracleDbType.Int64,     eI.TotalVolumeTraded,                ParameterDirection.Input)
                    ,new OracleParameter($"p{__AGROSSTRADEAMT}",                    OracleDbType.Decimal,   eI.GrossTradeAmt,                    ParameterDirection.Input)
                    ,new OracleParameter($"p{__ACONTAUCTACCTRDVOL}",                OracleDbType.Int64,     eI.ContauctAccTrdvol,                ParameterDirection.Input)
					,new OracleParameter($"p{__ACONTAUCTACCTRDVAL}",                OracleDbType.Decimal,   eI.ContauctAccTrdval,                ParameterDirection.Input)
					,new OracleParameter($"p{__ABLKTRDACCTRDVOL}",                  OracleDbType.Int64,     eI.BlktrdAccTrdvol,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ABLKTRDACCTRDVAL}",                  OracleDbType.Decimal,   eI.BlktrdAccTrdval,                  ParameterDirection.Input)
					,new OracleParameter($"p{__AFLUCTUATIONUPPERLIMITISSUECOUNT}",  OracleDbType.Int64,     eI.FluctuationUpperLimitIssueCount,  ParameterDirection.Input)
					,new OracleParameter($"p{__AFLUCTUATIONUPISSUECOUNT}",          OracleDbType.Int64,     eI.FluctuationUpIssueCount,          ParameterDirection.Input)
					,new OracleParameter($"p{__AFLUCTUATIONSTEADINESSISSUECOUNT}",  OracleDbType.Int64,     eI.FluctuationSteadinessIssueCount,  ParameterDirection.Input)
					,new OracleParameter($"p{__AFLUCTUATIONDOWNISSUECOUNT}",        OracleDbType.Int64,     eI.FluctuationDownIssueCount,        ParameterDirection.Input)
					,new OracleParameter($"p{__AFLUCTUATIONLOWERLIMITISSUECOUNT}",  OracleDbType.Int64,     eI.FluctuationLowerLimitIssueCount,  ParameterDirection.Input)
					,new OracleParameter($"p{__AFLUCTUATIONUPISSUEVOLUME}",         OracleDbType.Int64,     eI.FluctuationUpIssueVolume,         ParameterDirection.Input)
					,new OracleParameter($"p{__AFLUCTUATIONDOWNISSUEVOLUME}",       OracleDbType.Int64,     eI.FluctuationDownIssueVolume,       ParameterDirection.Input)
					,new OracleParameter($"p{__AFLUCTUATIONSTEADINESSISSUEVOLUME}", OracleDbType.Int64,     eI.FluctuationSteadinessIssueVolume, ParameterDirection.Input)
                    ,new OracleParameter($"p{__ACHECKSUM}",                         OracleDbType.Varchar2,  eI.CheckSum,                         ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",                        OracleDbType.Int64,     null,                                ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",                        OracleDbType.Varchar2,  500, null,                           ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
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
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateTopNMembersperSymbol;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",     OracleDbType.Varchar2,  eTNMPS.BeginString,     ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",      OracleDbType.Int64,     eTNMPS.BodyLength,      ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",         OracleDbType.Varchar2,  eTNMPS.MsgType,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",    OracleDbType.Varchar2,  eTNMPS.SenderCompID,    ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",    OracleDbType.Varchar2,  eTNMPS.TargetCompID,    ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",       OracleDbType.Int64,     eTNMPS.MsgSeqNum,       ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",     OracleDbType.Varchar2,  eTNMPS.SendingTime,     ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",        OracleDbType.Varchar2,  eTNMPS.MarketID,        ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",          OracleDbType.Varchar2,  eTNMPS.Symbol,          ParameterDirection.Input)
					,new OracleParameter($"p{__ATOTNUMREPORTS}",   OracleDbType.Int64,     eTNMPS.TotNumReports,   ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLRANKSEQ}",     OracleDbType.Int64,     eTNMPS.SellRankSeq,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLMEMBERNO}",    OracleDbType.Varchar2,  eTNMPS.SellMemberNo,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYRANKSEQ}",      OracleDbType.Int64,     eTNMPS.BuyRankSeq,      ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYMEMBERNO}",     OracleDbType.Varchar2,  eTNMPS.BuyMemberNo,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLVOLUME}",      OracleDbType.Int64,     eTNMPS.SellVolume,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLTRADEAMOUNT}", OracleDbType.Decimal,   eTNMPS.SellTradeAmount, ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYVOLUME}",       OracleDbType.Int64,     eTNMPS.BuyVolume,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYTRADEDAMOUNT}", OracleDbType.Decimal,   eTNMPS.BuyTradedAmount, ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",        OracleDbType.Varchar2,  eTNMPS.CheckSum,        ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",       OracleDbType.Int64,     null,                   ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",       OracleDbType.Varchar2,  500, null,              ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
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
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateOpenInterest;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",     OracleDbType.Varchar2,  eOI.BeginString,     ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",      OracleDbType.Int64,     eOI.BodyLength,      ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",         OracleDbType.Varchar2,  eOI.MsgType,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",    OracleDbType.Varchar2,  eOI.SenderCompID,    ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",    OracleDbType.Varchar2,  eOI.TargetCompID,    ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",       OracleDbType.Int64,     eOI.MsgSeqNum,       ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",     OracleDbType.Varchar2,  eOI.SendingTime,     ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",        OracleDbType.Varchar2,  eOI.MarketID,        ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",          OracleDbType.Varchar2,  eOI.Symbol,          ParameterDirection.Input)
					,new OracleParameter($"p{__ATRADEDATE}",       OracleDbType.Varchar2,  eOI.TradeDate,       ParameterDirection.Input)
					,new OracleParameter($"p{__AOPENINTERESTQTY}", OracleDbType.Int64,     eOI.OpenInterestQty, ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",        OracleDbType.Varchar2,  eOI.CheckSum,        ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",       OracleDbType.Int64,     null,                   ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",       OracleDbType.Varchar2,  500, null,              ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
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
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateDeemTradePrice;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",      OracleDbType.Varchar2,  eDTP.BeginString,         ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",       OracleDbType.Int64,     eDTP.BodyLength,          ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",          OracleDbType.Varchar2,  eDTP.MsgType,             ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",     OracleDbType.Varchar2,  eDTP.SenderCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",     OracleDbType.Varchar2,  eDTP.TargetCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",        OracleDbType.Int64,     eDTP.MsgSeqNum,           ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",      OracleDbType.Varchar2,  eDTP.SendingTime,         ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",         OracleDbType.Varchar2,  eDTP.MarketID,            ParameterDirection.Input)
					,new OracleParameter($"p{__ABOARDID}",          OracleDbType.Varchar2,  eDTP.BoardID,             ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",           OracleDbType.Varchar2,  eDTP.Symbol,              ParameterDirection.Input)
					,new OracleParameter($"p{__AEXPECTEDTRADEPX}",  OracleDbType.Decimal,   eDTP.ExpectedTradePx,     ParameterDirection.Input)
					,new OracleParameter($"p{__AEXPECTEDTRADEQTY}", OracleDbType.Int64,     eDTP.ExpectedTradeQty,    ParameterDirection.Input)
					,new OracleParameter($"p{__AEXPECTEDTRADEYIELD}",OracleDbType.Decimal,  eDTP.ExpectedTradeYield,  ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",         OracleDbType.Varchar2,  eDTP.CheckSum,            ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",        OracleDbType.Int64,     null,                     ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",        OracleDbType.Varchar2,  500, null,                ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
        //insert oracle table tbl_ig3_si
      

        /// <summary>
        /// 2020-08-15 hungtq 
        /// 4.21 Foreigner Order Limit
        /// insert data vao table tEForeignerOrderLimit
        /// chi insert 1 row 1 lan exec sp
        /// /// ============================================
        /// chu y phai khai bao size cua output var ten la "pReturnMess"
        /// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
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
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateForeignerOrderLimit;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
	                 new OracleParameter($"p{__ABEGINSTRING}",           OracleDbType.Varchar2,  eFOL.BeginString,           ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",            OracleDbType.Int64,     eFOL.BodyLength,            ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",               OracleDbType.Varchar2,  eFOL.MsgType,               ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",          OracleDbType.Varchar2,  eFOL.SenderCompID,          ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",          OracleDbType.Varchar2,  eFOL.TargetCompID,          ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",             OracleDbType.Int64,     eFOL.MsgSeqNum,             ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",           OracleDbType.Varchar2,  eFOL.SendingTime,           ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",              OracleDbType.Varchar2,  eFOL.MarketID,              ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",                OracleDbType.Varchar2,  eFOL.Symbol,                ParameterDirection.Input)
					,new OracleParameter($"p{__AFOREIGNERBUYPOSBLQTY}",  OracleDbType.Decimal,   eFOL.ForeignerBuyPosblQty,  ParameterDirection.Input)
					,new OracleParameter($"p{__AFOREIGNERORDERLIMITQTY}",OracleDbType.Int64,     eFOL.ForeignerOrderLimitQty,ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",              OracleDbType.Varchar2,  eFOL.CheckSum,              ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",             OracleDbType.Int64,     null,                       ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",             OracleDbType.Varchar2,  500, null,                  ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
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
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdatePriceLimitExpansion;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",      OracleDbType.Varchar2,  ePLE.BeginString,         ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",       OracleDbType.Int64,     ePLE.BodyLength,          ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",          OracleDbType.Varchar2,  ePLE.MsgType,             ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",     OracleDbType.Varchar2,  ePLE.SenderCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",     OracleDbType.Varchar2,  ePLE.TargetCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",        OracleDbType.Int64,     ePLE.MsgSeqNum,           ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",      OracleDbType.Varchar2,  ePLE.SendingTime,         ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",         OracleDbType.Varchar2,  ePLE.MarketID,            ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",           OracleDbType.Varchar2,  ePLE.Symbol,              ParameterDirection.Input)
					,new OracleParameter($"p{__ABOARDID}",          OracleDbType.Varchar2,  ePLE.BoardID,              ParameterDirection.Input)
					,new OracleParameter($"p{__ATRANSACTTIME}",     OracleDbType.Varchar2,  ePLE.TransactTime,        ParameterDirection.Input)
					,new OracleParameter($"p{__AHIGHLIMITPRICE}",   OracleDbType.Decimal,   ePLE.HighLimitPrice,      ParameterDirection.Input)
					,new OracleParameter($"p{__ALOWLIMITPRICE}",    OracleDbType.Decimal,   ePLE.LowLimitPrice,       ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",         OracleDbType.Varchar2,  ePLE.CheckSum,            ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",        OracleDbType.Int64,     null,                     ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",        OracleDbType.Varchar2,  500, null,                ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
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
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateETFiNav;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",      OracleDbType.Varchar2,  eEiN.BeginString,         ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",       OracleDbType.Int64,     eEiN.BodyLength,          ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",          OracleDbType.Varchar2,  eEiN.MsgType,             ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",     OracleDbType.Varchar2,  eEiN.SenderCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",     OracleDbType.Varchar2,  eEiN.TargetCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",        OracleDbType.Int64,     eEiN.MsgSeqNum,           ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",      OracleDbType.Varchar2,  eEiN.SendingTime,         ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",         OracleDbType.Varchar2,  eEiN.MarketID,            ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",           OracleDbType.Varchar2,  eEiN.Symbol,              ParameterDirection.Input)
					,new OracleParameter($"p{__ATRANSACTTIME}",     OracleDbType.Varchar2,  eEiN.TransactTime,        ParameterDirection.Input)
					,new OracleParameter($"p{__AINAVVALUE}",        OracleDbType.Decimal,   eEiN.iNAVvalue,           ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",         OracleDbType.Varchar2,  eEiN.CheckSum,            ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",        OracleDbType.Int64,     null,                     ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",        OracleDbType.Varchar2,  500, null,                ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
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
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateETFiIndex;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",      OracleDbType.Varchar2,  eEiI.BeginString,         ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",       OracleDbType.Int64,     eEiI.BodyLength,          ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",          OracleDbType.Varchar2,  eEiI.MsgType,             ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",     OracleDbType.Varchar2,  eEiI.SenderCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",     OracleDbType.Varchar2,  eEiI.TargetCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",        OracleDbType.Int64,     eEiI.MsgSeqNum,           ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",      OracleDbType.Varchar2,  eEiI.SendingTime,         ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",         OracleDbType.Varchar2,  eEiI.MarketID,            ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",           OracleDbType.Varchar2,  eEiI.Symbol,              ParameterDirection.Input)
					,new OracleParameter($"p{__ATRANSACTTIME}",     OracleDbType.Varchar2,  eEiI.TransactTime,        ParameterDirection.Input)
					,new OracleParameter($"p{__AVALUEINDEXES}",     OracleDbType.Decimal,   eEiI.ValueIndexes,        ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",         OracleDbType.Varchar2,  eEiI.CheckSum,            ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",        OracleDbType.Int64,     null,                     ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",        OracleDbType.Varchar2,  500, null,                ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
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
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateETFTrackingError;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",      OracleDbType.Varchar2,  eETE.BeginString,         ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",       OracleDbType.Int64,     eETE.BodyLength,          ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",          OracleDbType.Varchar2,  eETE.MsgType,             ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",     OracleDbType.Varchar2,  eETE.SenderCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",     OracleDbType.Varchar2,  eETE.TargetCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",        OracleDbType.Int64,     eETE.MsgSeqNum,           ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",      OracleDbType.Varchar2,  eETE.SendingTime,         ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",         OracleDbType.Varchar2,  eETE.MarketID,            ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",           OracleDbType.Varchar2,  eETE.Symbol,              ParameterDirection.Input)
					,new OracleParameter($"p{__ATRADEDATE}",        OracleDbType.Varchar2,  eETE.TradeDate,           ParameterDirection.Input)
					,new OracleParameter($"p{__ATRACKINGERROR}",    OracleDbType.Decimal,   eETE.TrackingError,       ParameterDirection.Input)
					,new OracleParameter($"p{__ADISPARATERATIO}",   OracleDbType.Decimal,   eETE.DisparateRatio,      ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",         OracleDbType.Varchar2,  eETE.CheckSum,            ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",        OracleDbType.Int64,     null,                     ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",        OracleDbType.Varchar2,  500, null,                ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// 4.26 TopNSymbolswithTradingQuantity
		/// insert data vao table tTopNSymbolswithTradingQuantity
		/// chi insert 1 row 1 lan exec sp
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTopNSymbolswithTradingQuantity(ETopNSymbolsWithTradingQuantity eTNSWTQ, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eTNSWTQ={this._cS6GApp.Common.SerializeObject(eTNSWTQ)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateTopNSymbolswithTradingQuantity;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",      OracleDbType.Varchar2,  eTNSWTQ.BeginString,         ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",       OracleDbType.Int64,     eTNSWTQ.BodyLength,          ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",          OracleDbType.Varchar2,  eTNSWTQ.MsgType,             ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",     OracleDbType.Varchar2,  eTNSWTQ.SenderCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",     OracleDbType.Varchar2,  eTNSWTQ.TargetCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",        OracleDbType.Int64,     eTNSWTQ.MsgSeqNum,           ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",      OracleDbType.Varchar2,  eTNSWTQ.SendingTime,         ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",         OracleDbType.Varchar2,  eTNSWTQ.MarketID,            ParameterDirection.Input)
					,new OracleParameter($"p{__ATOTNUMREPORTS}",    OracleDbType.Int64,     eTNSWTQ.TotNumReports,       ParameterDirection.Input)
					,new OracleParameter($"p{__ARANK}",             OracleDbType.Int64,     eTNSWTQ.Rank,                ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",           OracleDbType.Varchar2,  eTNSWTQ.Symbol,              ParameterDirection.Input)
					,new OracleParameter($"p{__AMDENTRYSIZE}",      OracleDbType.Int64,     eTNSWTQ.MDEntrySize,         ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",         OracleDbType.Varchar2,  eTNSWTQ.CheckSum,            ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",        OracleDbType.Int64,     null,                        ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",        OracleDbType.Varchar2,  500, null,                   ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTopNSymbolswithCurrentPrice(ETopNSymbolsWithCurrentPrice eTNSWCP, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eTNSWCP={this._cS6GApp.Common.SerializeObject(eTNSWCP)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateTopNSymbolswithCurrentPrice;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",      OracleDbType.Varchar2,  eTNSWCP.BeginString,         ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",       OracleDbType.Int64,     eTNSWCP.BodyLength,          ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",          OracleDbType.Varchar2,  eTNSWCP.MsgType,             ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",     OracleDbType.Varchar2,  eTNSWCP.SenderCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",     OracleDbType.Varchar2,  eTNSWCP.TargetCompID,        ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",        OracleDbType.Int64,     eTNSWCP.MsgSeqNum,           ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",      OracleDbType.Varchar2,  eTNSWCP.SendingTime,         ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",         OracleDbType.Varchar2,  eTNSWCP.MarketID,            ParameterDirection.Input)
					,new OracleParameter($"p{__ATOTNUMREPORTS}",    OracleDbType.Int64,     eTNSWCP.TotNumReports,       ParameterDirection.Input)
					,new OracleParameter($"p{__ARANK}",             OracleDbType.Int64,     eTNSWCP.Rank,                ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",           OracleDbType.Varchar2,  eTNSWCP.Symbol,              ParameterDirection.Input)
					,new OracleParameter($"p{__AMDENTRYPX}",        OracleDbType.Decimal,   eTNSWCP.MDEntryPx,           ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",         OracleDbType.Varchar2,  eTNSWCP.CheckSum,            ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",        OracleDbType.Int64,     null,                        ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",        OracleDbType.Varchar2,  500, null,                   ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTopNSymbolswithHighRatioofPrice(ETopNSymbolsWithHighRatioOfPrice eTNSWHROP, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eTNSWHROP={this._cS6GApp.Common.SerializeObject(eTNSWHROP)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateTopNSymbolswithHighRatioofPrice;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
			         new OracleParameter($"p{__ABEGINSTRING}",          OracleDbType.Varchar2,  eTNSWHROP.BeginString,          ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",           OracleDbType.Int64,     eTNSWHROP.BodyLength,           ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",              OracleDbType.Varchar2,  eTNSWHROP.MsgType,              ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",         OracleDbType.Varchar2,  eTNSWHROP.SenderCompID,         ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",         OracleDbType.Varchar2,  eTNSWHROP.TargetCompID,         ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",            OracleDbType.Int64,     eTNSWHROP.MsgSeqNum,            ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",          OracleDbType.Varchar2,  eTNSWHROP.SendingTime,          ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",             OracleDbType.Varchar2,  eTNSWHROP.MarketID,             ParameterDirection.Input)
					,new OracleParameter($"p{__ATOTNUMREPORTS}",        OracleDbType.Int64,     eTNSWHROP.TotNumReports,        ParameterDirection.Input)
					,new OracleParameter($"p{__ARANK}",                 OracleDbType.Int64,     eTNSWHROP.Rank,                 ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",               OracleDbType.Varchar2,  eTNSWHROP.Symbol,               ParameterDirection.Input)
					,new OracleParameter($"p{__APRICEFLUCTUATIONRATIO}",OracleDbType.Decimal,   eTNSWHROP.PriceFluctuationRatio,ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",             OracleDbType.Varchar2,  eTNSWHROP.CheckSum,             ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",            OracleDbType.Int64,     null,                           ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",            OracleDbType.Varchar2,  500, null,                      ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// insert data vao table tTopNSymbolswithlowRatioofPrice
		/// chi insert 1 row 1 lan exec sp
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTopNSymbolswithLowRatioofPrice(ETopNSymbolsWithLowRatioOfPrice eTNSWLROP, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eTNSWHROP={this._cS6GApp.Common.SerializeObject(eTNSWLROP)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateTopNSymbolswithLowRatioofPrice;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
			         new OracleParameter($"p{__ABEGINSTRING}",          OracleDbType.Varchar2,  eTNSWLROP.BeginString,          ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",           OracleDbType.Int64,     eTNSWLROP.BodyLength,           ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",              OracleDbType.Varchar2,  eTNSWLROP.MsgType,              ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",         OracleDbType.Varchar2,  eTNSWLROP.SenderCompID,         ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",         OracleDbType.Varchar2,  eTNSWLROP.TargetCompID,         ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",            OracleDbType.Int64,     eTNSWLROP.MsgSeqNum,            ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",          OracleDbType.Varchar2,  eTNSWLROP.SendingTime,          ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",             OracleDbType.Varchar2,  eTNSWLROP.MarketID,             ParameterDirection.Input)
					,new OracleParameter($"p{__ATOTNUMREPORTS}",        OracleDbType.Int64,     eTNSWLROP.TotNumReports,        ParameterDirection.Input)
					,new OracleParameter($"p{__ARANK}",                 OracleDbType.Int64,     eTNSWLROP.Rank,                 ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",               OracleDbType.Varchar2,  eTNSWLROP.Symbol,               ParameterDirection.Input)
					,new OracleParameter($"p{__APRICEFLUCTUATIONRATIO}",OracleDbType.Decimal,   eTNSWLROP.PriceFluctuationRatio,ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",             OracleDbType.Varchar2,  eTNSWLROP.CheckSum,             ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",            OracleDbType.Int64,     null,                           ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",            OracleDbType.Varchar2,  500, null,                      ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTradingResultofForeignInvestors(ETradingResultOfForeignInvestors eTRFI, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eTRFI={this._cS6GApp.Common.SerializeObject(eTRFI)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateTradingResultofForeignInvestors;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
		             new OracleParameter($"p{__ABEGINSTRING}",           OracleDbType.Varchar2,  eTRFI.BeginString,              ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",            OracleDbType.Int64,     eTRFI.BodyLength,               ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",               OracleDbType.Varchar2,  eTRFI.MsgType,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",          OracleDbType.Varchar2,  eTRFI.SenderCompID,             ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",          OracleDbType.Varchar2,  eTRFI.TargetCompID,             ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",             OracleDbType.Int64,     eTRFI.MsgSeqNum,                ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",           OracleDbType.Varchar2,  eTRFI.SendingTime,              ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",              OracleDbType.Varchar2,  eTRFI.MarketID,                 ParameterDirection.Input)
					,new OracleParameter($"p{__ABOARDID}",               OracleDbType.Varchar2,  eTRFI.BoardID,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",                OracleDbType.Varchar2,  eTRFI.Symbol,                   ParameterDirection.Input)
					,new OracleParameter($"p{__ATRADINGSESSIONID}",      OracleDbType.Varchar2,  eTRFI.TradingSessionID,         ParameterDirection.Input)
					,new OracleParameter($"p{__ATRANSACTTIME}",          OracleDbType.Varchar2,  eTRFI.TransactTime,             ParameterDirection.Input)
					,new OracleParameter($"p{__AFORNINVESTTYPECODE}",    OracleDbType.Varchar2,  eTRFI.FornInvestTypeCode,       ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLVOLUME}",            OracleDbType.Int64,     eTRFI.SellVolume,               ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLTRADEAMOUNT}",       OracleDbType.Decimal,   eTRFI.SellTradeAmount,          ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYVOLUME}",             OracleDbType.Int64,     eTRFI.BuyVolume,                ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYTRADEDAMOUNT}",       OracleDbType.Decimal,   eTRFI.BuyTradedAmount,          ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLVOLUMETOTAL}",       OracleDbType.Int64,     eTRFI.SellVolumeTotal,          ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLTRADEAMOUNTTOTAL}",  OracleDbType.Decimal,   eTRFI.SellTradeAmountTotal,     ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYVOLUMETOTAL}",        OracleDbType.Int64,     eTRFI.BuyVolumeTotal,           ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYTRADEDAMOUNTTOTAL}",  OracleDbType.Decimal,   eTRFI.BuyTradeAmountTotal,      ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",              OracleDbType.Varchar2,  eTRFI.CheckSum,                 ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",             OracleDbType.Int64,     null,                           ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",             OracleDbType.Varchar2,  500, null,                      ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateDisclosure(EDisclosure eD, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eD={this._cS6GApp.Common.SerializeObject(eD)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateDisclosure;
				EDalResult result = null;

				eD.SymbolName = this._cS6GApp.Common.ProcessSqlEscapeChar(eD.SymbolName);

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",           OracleDbType.Varchar2,  eD.BeginString,              ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",            OracleDbType.Int64,     eD.BodyLength,               ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",               OracleDbType.Varchar2,  eD.MsgType,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",          OracleDbType.Varchar2,  eD.SenderCompID,             ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",          OracleDbType.Varchar2,  eD.TargetCompID,             ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",             OracleDbType.Int64,     eD.MsgSeqNum,                ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",           OracleDbType.Varchar2,  eD.SendingTime,              ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",              OracleDbType.Varchar2,  eD.MarketID,                 ParameterDirection.Input)
					,new OracleParameter($"p{__ASECURITYEXCHANGE}",      OracleDbType.Varchar2,  eD.SecurityExchange,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",                OracleDbType.Varchar2,  eD.Symbol,                   ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOLNAME}",            OracleDbType.Varchar2,  eD.SymbolName,               ParameterDirection.Input)
					,new OracleParameter($"p{__ADISCLOSUREID}",          OracleDbType.Varchar2,  eD.DisclosureID,             ParameterDirection.Input)
					,new OracleParameter($"p{__ATOTALMSGNO}",            OracleDbType.Int64,     eD.TotalMsgNo,               ParameterDirection.Input)
					,new OracleParameter($"p{__ACURRENTMSGNO}",          OracleDbType.Int64,     eD.CurrentMsgNo,             ParameterDirection.Input)
					,new OracleParameter($"p{__ALANQUAGECATEGORY}",      OracleDbType.Varchar2,  eD.LanquageCategory,         ParameterDirection.Input)
					,new OracleParameter($"p{__ADATACATEGORY}",          OracleDbType.Varchar2,  eD.DataCategory,             ParameterDirection.Input)
					,new OracleParameter($"p{__APUBLICINFORMATIONDATE}", OracleDbType.Varchar2,  eD.PublicInformationDate,    ParameterDirection.Input)
					,new OracleParameter($"p{__ATRANSMISSIONDATE}",      OracleDbType.Varchar2,  eD.TransmissionDate,         ParameterDirection.Input)
					,new OracleParameter($"p{__APROCESSTYPE}",           OracleDbType.Varchar2,  eD.ProcessType,              ParameterDirection.Input)
					,new OracleParameter($"p{__AHEADLINE}",              OracleDbType.Varchar2,  eD.Headline,                 ParameterDirection.Input)
					,new OracleParameter($"p{__ABODY}",                  OracleDbType.Varchar2,  eD.Body,                     ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",              OracleDbType.Varchar2,  eD.CheckSum,                 ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",             OracleDbType.Int64,     null,                        ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",             OracleDbType.Varchar2,  500, null,                   ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eTNMPS"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdateTimeStampPolling(ETimeStampPolling eTSP, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} eD={this._cS6GApp.Common.SerializeObject(eTSP)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdateTimeStampPolling;
				EDalResult result = null;


				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",           OracleDbType.Varchar2,  eTSP.BeginString,              ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",            OracleDbType.Int64,     eTSP.BodyLength,               ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",               OracleDbType.Varchar2,  eTSP.MsgType,                  ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",          OracleDbType.Varchar2,  eTSP.SenderCompID,             ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",          OracleDbType.Varchar2,  eTSP.TargetCompID,             ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",             OracleDbType.Int64,     eTSP.MsgSeqNum,                ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",           OracleDbType.Varchar2,  TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(eTSP.SendingTime, "yyyy-MM-dd HH:mm:ss.fff", null), timeZone).ToString("yyyy-MM-dd HH:mm:ss.fff"),              ParameterDirection.Input)
					,new OracleParameter($"p{__ATRANSACTTIME}",          OracleDbType.Varchar2,  eTSP.TransactTime,             ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",              OracleDbType.Varchar2,  eTSP.CheckSum,                 ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",             OracleDbType.Int64,     null,                        ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",             OracleDbType.Varchar2,  500, null,                   ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
		/// 4.10 price
		/// insert data vao table tRandomEnd
		/// chi insert 1 row 1 lan exec sp
		/// /// ============================================
		/// chu y phai khai bao size cua output var ten la "pReturnMess"
		/// neu ko khai bao size thi luon gap error ORA-06502: PL/SQL: numeric or value error: character string buffer too small
		/// </summary>
		/// <param name="eSIN"></param>
		/// <returns></returns>
		public async Task<EDalResult> UpdatePriceRecoveryAll(EPriceRecovery ePR, bool getScriptOnly = false)
		{
			// log input
			TExecutionContext ec = this._cS6GApp.DebugLogger.WriteBufferBegin($"{EGlobalConfig.__STRING_BEFORE} ePR={_cS6GApp.Common.SerializeObject(ePR)}", true);

			try
			{
				// 0. init
				IOracle oracle = new COracle(this._cS6GApp, this._ePriceConfig.ConnectionOracle);
				string spName = this._ePriceConfig.SpMddsOracleUpdatePriceRecoveryAll;
				EDalResult result = null;

				// 1. input            
				OracleParameter[] paramArrary = new OracleParameter[]
				{
					 new OracleParameter($"p{__ABEGINSTRING}",        OracleDbType.Varchar2,    ePR.BeginString,        ParameterDirection.Input)
 					,new OracleParameter($"p{__ABODYLENGTH}",         OracleDbType.Int64,       ePR.BodyLength,         ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGTYPE}",            OracleDbType.Varchar2,    ePR.MsgType,            ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDERCOMPID}",       OracleDbType.Varchar2,    ePR.SenderCompID,       ParameterDirection.Input)
					,new OracleParameter($"p{__ATARGETCOMPID}",       OracleDbType.Varchar2,    ePR.TargetCompID,       ParameterDirection.Input)
					,new OracleParameter($"p{__AMSGSEQNUM}",          OracleDbType.NVarchar2,   ePR.MsgSeqNum,          ParameterDirection.Input)
					,new OracleParameter($"p{__ASENDINGTIME}",        OracleDbType.Varchar2,    ePR.SendingTime,        ParameterDirection.Input)
					,new OracleParameter($"p{__AMARKETID}",           OracleDbType.Varchar2,    ePR.MarketID,           ParameterDirection.Input)
					,new OracleParameter($"p{__ABOARDID}",            OracleDbType.Varchar2,    ePR.BoardID,            ParameterDirection.Input)
					,new OracleParameter($"p{__ATRADINGSESSIONID}",   OracleDbType.Varchar2,    ePR.TradingSessionID,   ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOL}",             OracleDbType.Varchar2,    ePR.Symbol,             ParameterDirection.Input)
					,new OracleParameter($"p{__AOPNPX}",              OracleDbType.Decimal,     ePR.OpnPx,              ParameterDirection.Input)
					,new OracleParameter($"p{__ATRDSESSNHIGHPX}",     OracleDbType.Decimal,     ePR.TrdSessnHighPx,     ParameterDirection.Input)
					,new OracleParameter($"p{__ATRDSESSNLOWPX}",      OracleDbType.Decimal,     ePR.TrdSessnLowPx,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASYMBOLCLOSEINFOPX}",  OracleDbType.Decimal,     ePR.SymbolCloseInfoPx,  ParameterDirection.Input)
					//,new OracleParameter($"p{__AOPNPXYLD}",           OracleDbType.Decimal,     ePR.OpnPxYld,           ParameterDirection.Input)
					//,new OracleParameter($"p{__ATRDSESSNHIGHPXYLD}",  OracleDbType.Decimal,     ePR.TrdSessnHighPxYld,  ParameterDirection.Input)
					//,new OracleParameter($"p{__ATRDSESSNLOWPXYLD}",   OracleDbType.Decimal,     ePR.TrdSessnLowPxYld,   ParameterDirection.Input)
					//,new OracleParameter($"p{__ACLSPXYLD}",           OracleDbType.Decimal,     ePR.ClsPxYld,           ParameterDirection.Input)
					,new OracleParameter($"p{__ATOTALVOLUMETRADED}",  OracleDbType.Int64,       ePR.TotalVolumeTraded,  ParameterDirection.Input)
					,new OracleParameter($"p{__AGROSSTRADEAMT}",      OracleDbType.Decimal,     ePR.GrossTradeAmt,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLTOTORDERQTY}",    OracleDbType.Int64,       ePR.SellTotOrderQty,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYTOTORDERQTY}",     OracleDbType.Int64,       ePR.BuyTotOrderQty,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLVALIDORDERCNT}",  OracleDbType.Int64,       ePR.SellValidOrderCnt,  ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYVALIDORDERCNT}",   OracleDbType.Int64,       ePR.BuyValidOrderCnt,   ParameterDirection.Input)
					,new OracleParameter($"p{__ANOMDENTRIES}",        OracleDbType.Int64,       ePR.NoMDEntries,        ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE1}",          OracleDbType.Decimal,     ePR.BuyPrice1,          ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYQUANTITY1}",       OracleDbType.Int64,       ePR.BuyQuantity1,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE1_NOO}",      OracleDbType.Int64,       ePR.BuyPrice1_NOO,      ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE1_MDEY}",     OracleDbType.Decimal,     ePR.BuyPrice10_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE1_MDEMMS}",   OracleDbType.Int64,       ePR.BuyPrice1_MDEMMS,   ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE1}",         OracleDbType.Decimal,     ePR.SellPrice1,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLQUANTITY1}",      OracleDbType.Int64,       ePR.SellQuantity1,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE1_NOO}",     OracleDbType.Int64,       ePR.SellPrice1_NOO,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE1_MDEY}",    OracleDbType.Decimal,     ePR.SellPrice1_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE1_MDEMMS}",  OracleDbType.Int64,       ePR.SellPrice1_MDEMMS,  ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE2}",          OracleDbType.Decimal,     ePR.BuyPrice2,          ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYQUANTITY2}",       OracleDbType.Int64,       ePR.BuyQuantity2,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE2_NOO}",      OracleDbType.Int64,       ePR.BuyPrice2_NOO,      ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE2_MDEY}",     OracleDbType.Decimal,     ePR.BuyPrice2_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE2_MDEMMS}",   OracleDbType.Int64,       ePR.BuyPrice2_MDEMMS,   ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE2}",         OracleDbType.Decimal,     ePR.SellPrice2,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLQUANTITY2}",      OracleDbType.Int64,       ePR.SellQuantity2,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE2_NOO}",     OracleDbType.Int64,       ePR.SellPrice2_NOO,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE2_MDEY}",    OracleDbType.Decimal,     ePR.SellPrice2_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE2_MDEMMS}",  OracleDbType.Int64,       ePR.SellPrice2_MDEMMS,  ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE3}",          OracleDbType.Decimal,     ePR.BuyPrice3,          ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYQUANTITY3}",       OracleDbType.Int64,       ePR.BuyQuantity3,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE3_NOO}",      OracleDbType.Int64,       ePR.BuyPrice3_NOO,      ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE3_MDEY}",     OracleDbType.Decimal,     ePR.BuyPrice3_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE3_MDEMMS}",   OracleDbType.Int64,       ePR.BuyPrice3_MDEMMS,   ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE3}",         OracleDbType.Decimal,     ePR.SellPrice3,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLQUANTITY3}",      OracleDbType.Int64,       ePR.SellQuantity3,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE3_NOO}",     OracleDbType.Int64,       ePR.SellPrice3_NOO,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE3_MDEY}",    OracleDbType.Decimal,     ePR.SellPrice3_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE3_MDEMMS}",  OracleDbType.Int64,       ePR.SellPrice3_MDEMMS,  ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE4}",          OracleDbType.Decimal,     ePR.BuyPrice4,          ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYQUANTITY4}",       OracleDbType.Int64,       ePR.BuyQuantity4,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE4_NOO}",      OracleDbType.Int64,       ePR.BuyPrice4_NOO,      ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE4_MDEY}",     OracleDbType.Decimal,     ePR.BuyPrice4_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE4_MDEMMS}",   OracleDbType.Int64,       ePR.BuyPrice4_MDEMMS,   ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE4}",         OracleDbType.Decimal,     ePR.SellPrice4,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLQUANTITY4}",      OracleDbType.Int64,       ePR.SellQuantity4,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE4_NOO}",     OracleDbType.Int64,       ePR.SellPrice4_NOO,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE4_MDEY}",    OracleDbType.Decimal,     ePR.SellPrice4_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE4_MDEMMS}",  OracleDbType.Int64,       ePR.SellPrice4_MDEMMS,  ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE5}",          OracleDbType.Decimal,     ePR.BuyPrice5,          ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYQUANTITY5}",       OracleDbType.Int64,       ePR.BuyQuantity5,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE5_NOO}",      OracleDbType.Int64,       ePR.BuyPrice5_NOO,      ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE5_MDEY}",     OracleDbType.Decimal,     ePR.BuyPrice5_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE5_MDEMMS}",   OracleDbType.Int64,       ePR.BuyPrice5_MDEMMS,   ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE5}",         OracleDbType.Decimal,     ePR.SellPrice5,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLQUANTITY5}",      OracleDbType.Int64,       ePR.SellQuantity5,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE5_NOO}",     OracleDbType.Int64,       ePR.SellPrice5_NOO,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE5_MDEY}",    OracleDbType.Decimal,     ePR.SellPrice5_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE5_MDEMMS}",  OracleDbType.Int64,       ePR.SellPrice5_MDEMMS,  ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE6}",          OracleDbType.Decimal,     ePR.BuyPrice6,          ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYQUANTITY6}",       OracleDbType.Int64,       ePR.BuyQuantity6,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE6_NOO}",      OracleDbType.Int64,       ePR.BuyPrice6_NOO,      ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE6_MDEY}",     OracleDbType.Decimal,     ePR.BuyPrice6_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE6_MDEMMS}",   OracleDbType.Int64,       ePR.BuyPrice6_MDEMMS,   ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE6}",         OracleDbType.Decimal,     ePR.SellPrice6,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLQUANTITY6}",      OracleDbType.Int64,       ePR.SellQuantity6,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE6_NOO}",     OracleDbType.Int64,       ePR.SellPrice6_NOO,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE6_MDEY}",    OracleDbType.Decimal,     ePR.SellPrice6_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE6_MDEMMS}",  OracleDbType.Int64,       ePR.SellPrice6_MDEMMS,  ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE7}",          OracleDbType.Decimal,     ePR.BuyPrice7,          ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYQUANTITY7}",       OracleDbType.Int64,       ePR.BuyQuantity7,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE7_NOO}",      OracleDbType.Int64,       ePR.BuyPrice7_NOO,      ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE7_MDEY}",     OracleDbType.Decimal,     ePR.BuyPrice7_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE7_MDEMMS}",   OracleDbType.Int64,       ePR.BuyPrice7_MDEMMS,   ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE7}",         OracleDbType.Decimal,     ePR.SellPrice7,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLQUANTITY7}",      OracleDbType.Int64,       ePR.SellQuantity7,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE7_NOO}",     OracleDbType.Int64,       ePR.SellPrice7_NOO,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE7_MDEY}",    OracleDbType.Decimal,     ePR.SellPrice7_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE7_MDEMMS}",  OracleDbType.Int64,       ePR.SellPrice7_MDEMMS,  ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE8}",          OracleDbType.Decimal,     ePR.BuyPrice8,          ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYQUANTITY8}",       OracleDbType.Int64,       ePR.BuyQuantity8,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE8_NOO}",      OracleDbType.Int64,       ePR.BuyPrice8_NOO,      ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE8_MDEY}",     OracleDbType.Decimal,     ePR.BuyPrice8_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE8_MDEMMS}",   OracleDbType.Int64,       ePR.BuyPrice8_MDEMMS,   ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE8}",         OracleDbType.Decimal,     ePR.SellPrice8,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLQUANTITY8}",      OracleDbType.Int64,       ePR.SellQuantity8,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE8_NOO}",     OracleDbType.Int64,       ePR.SellPrice8_NOO,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE8_MDEY}",    OracleDbType.Decimal,     ePR.SellPrice8_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE8_MDEMMS}",  OracleDbType.Int64,       ePR.SellPrice8_MDEMMS,  ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE9}",          OracleDbType.Decimal,     ePR.BuyPrice9,          ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYQUANTITY9}",       OracleDbType.Int64,       ePR.BuyQuantity9,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE9_NOO}",      OracleDbType.Int64,       ePR.BuyPrice9_NOO,      ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE9_MDEY}",     OracleDbType.Decimal,     ePR.BuyPrice9_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE9_MDEMMS}",   OracleDbType.Int64,       ePR.BuyPrice9_MDEMMS,   ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE9}",         OracleDbType.Decimal,     ePR.SellPrice9,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLQUANTITY9}",      OracleDbType.Int64,       ePR.SellQuantity9,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE9_NOO}",     OracleDbType.Int64,       ePR.SellPrice9_NOO,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE9_MDEY}",    OracleDbType.Decimal,     ePR.SellPrice9_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE9_MDEMMS}",  OracleDbType.Int64,       ePR.SellPrice9_MDEMMS,  ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE10}",          OracleDbType.Decimal,    ePR.BuyPrice10,          ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYQUANTITY10}",       OracleDbType.Int64,      ePR.BuyQuantity10,       ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE10_NOO}",      OracleDbType.Int64,      ePR.BuyPrice10_NOO,      ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE10_MDEY}",     OracleDbType.Decimal,    ePR.BuyPrice10_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ABUYPRICE10_MDEMMS}",   OracleDbType.Int64,      ePR.BuyPrice10_MDEMMS,   ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE10}",         OracleDbType.Decimal,    ePR.SellPrice10,         ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLQUANTITY10}",      OracleDbType.Int64,      ePR.SellQuantity10,      ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE10_NOO}",     OracleDbType.Int64,      ePR.SellPrice10_NOO,     ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE10_MDEY}",    OracleDbType.Decimal,    ePR.SellPrice10_MDEY,    ParameterDirection.Input)
					,new OracleParameter($"p{__ASELLPRICE10_MDEMMS}",  OracleDbType.Int64,      ePR.SellPrice10_MDEMMS,  ParameterDirection.Input)
					,new OracleParameter($"p{__AMATCHPRICE}",          OracleDbType.Decimal,    ePR.MatchPrice,          ParameterDirection.Input)
					,new OracleParameter($"p{__AMATCHQUANTITY}",       OracleDbType.Int64,      ePR.MatchQuantity,       ParameterDirection.Input)
					,new OracleParameter($"p{__AOPENPRICE}",          OracleDbType.Decimal,     ePR.OpenPrice,          ParameterDirection.Input)
					,new OracleParameter($"p{__ACLOSEPRICE}",        OracleDbType.Decimal,      ePR.ClosePrice,         ParameterDirection.Input)
					,new OracleParameter($"p{__AHIGHESTPRICE}",      OracleDbType.Decimal,      ePR.HighestPrice,       ParameterDirection.Input)
					,new OracleParameter($"p{__ALOWESTPRICE}",       OracleDbType.Decimal,      ePR.LowestPrice,        ParameterDirection.Input)
					//,new OracleParameter($"p{__REPEATINGDATAFIX}",    OracleDbType.Varchar2,    ePR.RepeatingDataFix,   ParameterDirection.Input)
					//,new OracleParameter($"p{__REPEATINGDATAJSON}",   OracleDbType.Varchar2,    ePR.RepeatingDataJson,  ParameterDirection.Input)
					,new OracleParameter($"p{__ACHECKSUM}",           OracleDbType.Varchar2,    ePR.CheckSum,           ParameterDirection.Input)
					,new OracleParameter($"p{__RETURNCODE}",          OracleDbType.Int64,       null,                  ParameterDirection.Output)
					,new OracleParameter($"p{__RETURNMESS}",          OracleDbType.Varchar2,    500, null,             ParameterDirection.Output) // 2020-07-27 14:47:53 ngocta2 phai khai bao size cua var nay
				};

				// ko exec sp, chi lay script de run bulk update sau nay 
				if (getScriptOnly)
				{
					return new EDalResult() { Code = EDalResult.__CODE_SUCCESS, Message = EGlobalConfig.__STRING_GET_SCRIPT, Data = oracle.GetScript(spName, paramArrary) };
				}

				// 2. main									
				result = await oracle.ExecuteSpNoQueryAsync(spName, paramArrary);

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
        public Task<EDalResult> UpdateDrvProductEventAll(EDrvProductEvent eDrvProductEvent, bool getScriptOnly = false)
        {
            throw new NotImplementedException();
        }
    }

}
