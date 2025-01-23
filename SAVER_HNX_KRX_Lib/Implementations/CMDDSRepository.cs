using CommonLib.Interfaces;
using MDDSCore.Messages;
using Microsoft.Extensions.Configuration;
using PriceLib;
using PriceLib.Implementations;
using PriceLib.Interfaces;
using BaseSaverLib.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SystemCore.Entities;
using SystemCore.Temporaries;
using Oracle.ManagedDataAccess.Client;
using System.Data.SqlClient;

namespace BaseSaverLib.Implementations
{
    public class CMDDSRepository : IMDDSRepository
    {
        // vars
        private readonly IS6GApp _app;
        private readonly IMDDS _oracle;
        private readonly IMDDS _mssql;
        private readonly EPriceConfig _priceConfig;
        private readonly OracleConnection _connectionOracle;
        private readonly SqlConnection _connectionSql;
        /// <summary>
        /// 2020-07-30 13:39:41 ngocta2
        /// constructor
        /// </summary>
        /// <param name="app"></param>
        /// <param name="repository"></param>
        public CMDDSRepository(IS6GApp app)
        {
            this._app = app;
            this._priceConfig = new EPriceConfig();
           // this._connectionOracle = oracle;
          //  this._connectionSql = sql;
            this._app.Configuration.GetSection(EPriceConfig.__SECTION_PRICECONFIG).Bind(_priceConfig);
            this._oracle = new CMDDSOracle(app, _priceConfig);
            this._mssql = new CMDDSMssql(app, _priceConfig);
        }


        /// <summary>
        /// 2020-08-04 14:11:01 ngocta2
        /// exec bulk script de insert nhanh data vao db
        /// </summary>
        /// <param name="mssqlScript"></param>
        /// <param name="oracleScript"></param>
        /// <returns></returns>
        public async Task<EDalResult> ExecBulkScript(string mssqlScript, string oracleScript)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin mssqlScript={mssqlScript}; oracleScript={oracleScript}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.ExecuteScript(mssqlScript));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.ExecuteScript(oracleScript));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EDalResult()
                {
                    Code = mssqlResult.Code + oracleResult.Code,
                    Message = mssqlResult.Message + "; " + oracleResult.Message,
                    Data = mssqlResult.Data
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return new EDalResult() { Code = EDalResult.__CODE_ERROR, Message = ex.Message, Data = null };
            }
        }

        public async Task<EDalResult> ExecBulkScript(string mssqlScript)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin mssqlScript={mssqlScript};", true);
            EDalResult mssqlResult = null;
            
            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.ExecuteScript(mssqlScript));

              
                // wait all
                await Task.WhenAll(mssqlTask);

                // return data
                return new EDalResult()
                {
                    Code = mssqlResult.Code ,
                    Message = mssqlResult.Message ,
                    Data = mssqlResult.Data
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return new EDalResult() { Code = EDalResult.__CODE_ERROR, Message = ex.Message, Data = null };
            }
        }

        /// <summary>
        /// 2020-07-30 16:54:01 ngocta2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eSD"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptSecurityDefinition(ESecurityDefinition eSD)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eSD={this._app.Common.SerializeObject(eSD)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;
            EDalResult oracleResult2 = null;
            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateSecurityDefinition(eSD, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateSecurityDefinition(eSD, true));
               // Task oracleTask2 = Task.Run(async () => oracleResult2 = await this._oracle.UpdateSecurityDefinitionIG3SI(eSD, true));
                
                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-12 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eSS"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptSecurityStatus(ESecurityStatus eSS)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eSS={this._app.Common.SerializeObject(eSS)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateSecurityStatus(eSS, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateSecurityStatus(eSS, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }
        public async Task<EBulkScript> GetScriptDrvProductEvent(EDrvProductEvent eDRV)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eP={this._app.Common.SerializeObject(eDRV)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;
            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateDrvProductEventAll(eDRV, true));
                // update vao ORACLE
                //Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdatePriceRecoveryAll(ePR, true));
                // wait all
                await Task.WhenAll(mssqlTask);
                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    //   OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-12 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eSIN"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptSecurityInformationNotification(ESecurityInformationNotification eSIN)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eSIN={this._app.Common.SerializeObject(eSIN)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateSecurityInformationNotification(eSIN, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateSecurityInformationNotification(eSIN, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-12 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eSCI"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptSymbolClosingInformation(ESymbolClosingInformation eSCI)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eSCI={this._app.Common.SerializeObject(eSCI)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateSymbolClosingInformation(eSCI, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateSymbolClosingInformation(eSCI, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-12 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eVI"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptVolatilityInterruption(EVolatilityInterruption eVI)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eVI={this._app.Common.SerializeObject(eVI)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateVolatilityInterruption(eVI, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateVolatilityInterruption(eVI, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-12 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eMMI"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptMarketMakerInformation(EMarketMakerInformation eMMI)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eMMI={this._app.Common.SerializeObject(eMMI)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateMarketMakerInformation(eMMI, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateMarketMakerInformation(eMMI, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-12 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eSE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptSymbolEvent(ESymbolEvent eSE)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eSE={this._app.Common.SerializeObject(eSE)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateSymbolEvent(eSE, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateSymbolEvent(eSE, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-12 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eSE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptIndexConstituentsInformation(EIndexConstituentsInformation eICI)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eICI={this._app.Common.SerializeObject(eICI)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateIndexConstituentsInformation(eICI, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateIndexConstituentsInformation(eICI, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-12 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eSE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptRandomEnd(ERandomEnd eRE)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eRE={this._app.Common.SerializeObject(eRE)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateRandomEnd(eRE, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateRandomEnd(eRE, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eSE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptInvestorperIndustry(EInvestorPerIndustry eIPI)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eIPI={this._app.Common.SerializeObject(eIPI)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateInvestorperIndustry(eIPI, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateInvestorperIndustry(eIPI, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        ///// <summary>
        ///// 2020-08-17 hungtq2
        ///// call DAL code de update data vao db mssql + oracle
        ///// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        ///// </summary>
        ///// <param name="eSE"></param>
        ///// <returns></returns>
        //public async Task<EBulkScript> GetScriptInvestorperIndustryBond(EInvestorPerIndustryBond eIPIB)
        //{
        //	TExecutionContext ec = this._s6GApp.DebugLogger.WriteBufferBegin($"begin eIPIB={this._s6GApp.Common.SerializeObject(eIPIB)}", true);
        //	EDalResult mssqlResult = null;
        //	EDalResult oracleResult = null;

        //	try
        //	{
        //		// update vao MSSQL
        //		Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateInvestorperIndustryBond(eIPIB, true));

        //		// update vao ORACLE
        //		//Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateInvestorperIndustryBond(eIPIB, true));

        //		// wait all
        //		await Task.WhenAll(mssqlTask);

        //		// return data
        //		return new EBulkScript()
        //		{
        //			MssqlScript = mssqlResult.Data.ToString(),
        //			OracleScript = oracleResult.Data.ToString()
        //		};
        //	}
        //	catch (Exception ex)
        //	{
        //		// log error + buffer data
        //		this._s6GApp.ErrorLogger.LogErrorContext(ex, ec);
        //		// return null
        //		return null;
        //	}
        //}


        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eSE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptIndex(EIndex eI)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eIPIB={this._app.Common.SerializeObject(eI)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateIndex(eI, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateIndex(eI, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eIPS"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptInvestorperSymbol(EInvestorPerSymbol eIPS)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eIPS={this._app.Common.SerializeObject(eIPS)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateInvestorperSymbol(eIPS, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateInvestorperSymbol(eIPS, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eTNMPS"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptTopNMembersperSymbol(ETopNMembersPerSymbol eTNMPS)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eTNMPS={this._app.Common.SerializeObject(eTNMPS)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateTopNMembersperSymbol(eTNMPS, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateTopNMembersperSymbol(eTNMPS, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eTNMPS"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptOpenInterest(EOpenInterest eOI)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eOI={this._app.Common.SerializeObject(eOI)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateOpenInterest(eOI, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateOpenInterest(eOI, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eTNMPS"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptDeemTradePrice(EDeemTradePrice eDTP)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eDTP={this._app.Common.SerializeObject(eDTP)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateDeemTradePrice(eDTP, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateDeemTradePrice(eDTP, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="eTNMPS"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptForeignerOrderLimit(EForeignerOrderLimit eFOL)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eFOL={this._app.Common.SerializeObject(eFOL)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateForeignerOrderLimit(eFOL, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateForeignerOrderLimit(eFOL, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptPriceLimitExpansion(EPriceLimitExpansion ePLE)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin ePLE={this._app.Common.SerializeObject(ePLE)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdatePriceLimitExpansion(ePLE, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdatePriceLimitExpansion(ePLE, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptETFiNav(EETFiNav eEiN)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eEiN={this._app.Common.SerializeObject(eEiN)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateETFiNav(eEiN, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateETFiNav(eEiN, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptETFiIndex(EETFiIndex eEiI)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eEiI={this._app.Common.SerializeObject(eEiI)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateETFiIndex(eEiI, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateETFiIndex(eEiI, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptETFTrackingError(EETFTrackingError eETE)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eETE={this._app.Common.SerializeObject(eETE)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateETFTrackingError(eETE, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateETFTrackingError(eETE, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptTopNSymbolswithTradingQuantity(ETopNSymbolsWithTradingQuantity eTNSWTQ)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eTNSWTQ={this._app.Common.SerializeObject(eTNSWTQ)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateTopNSymbolswithTradingQuantity(eTNSWTQ, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateTopNSymbolswithTradingQuantity(eTNSWTQ, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptTopNSymbolswithCurrentPrice(ETopNSymbolsWithCurrentPrice eTNSWCP)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eTNSWCP={this._app.Common.SerializeObject(eTNSWCP)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateTopNSymbolswithCurrentPrice(eTNSWCP, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateTopNSymbolswithCurrentPrice(eTNSWCP, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptTopNSymbolswithHighRatioofPrice(ETopNSymbolsWithHighRatioOfPrice ETNSWHROP)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin ETNSWHROP={this._app.Common.SerializeObject(ETNSWHROP)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateTopNSymbolswithHighRatioofPrice(ETNSWHROP, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateTopNSymbolswithHighRatioofPrice(ETNSWHROP, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptTopNSymbolswithLowRatioofPrice(ETopNSymbolsWithLowRatioOfPrice ETNSWLROP)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin ETNSWHROP={this._app.Common.SerializeObject(ETNSWLROP)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateTopNSymbolswithLowRatioofPrice(ETNSWLROP, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateTopNSymbolswithLowRatioofPrice(ETNSWLROP, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptTradingResultofForeignInvestors(ETradingResultOfForeignInvestors ETRFI)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin ETRFI={this._app.Common.SerializeObject(ETRFI)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateTradingResultofForeignInvestors(ETRFI, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateTradingResultofForeignInvestors(ETRFI, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptDisclosure(EDisclosure eD)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eD={this._app.Common.SerializeObject(eD)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateDisclosure(eD, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateDisclosure(eD, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptTimeStampPolling(ETimeStampPolling eTSP)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eD={this._app.Common.SerializeObject(eTSP)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdateTimeStampPolling(eTSP, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdateTimeStampPolling(eTSP, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptPriceAll(EPrice eP)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eP={this._app.Common.SerializeObject(eP)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;
            EDalResult oracleResult2 = null;
            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdatePriceAll(eP, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdatePriceAll(eP, true));

                //   Task oracleTask2 = Task.Run(async () => oracleResult2 = await this._oracle.UpdatePriceAllIG3SI(eP, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

        /// <summary>
        /// 2020-08-17 hungtq2
        /// call DAL code de update data vao db mssql + oracle
        /// chu y: chay song song ca 2 task update mssql + oracle de xong nhanh nhat co the
        /// </summary>
        /// <param name="ePLE"></param>
        /// <returns></returns>
        public async Task<EBulkScript> GetScriptPriceRecoveryAll(EPriceRecovery ePR)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"begin eP={this._app.Common.SerializeObject(ePR)}", true);
            EDalResult mssqlResult = null;
            EDalResult oracleResult = null;

            try
            {
                // update vao MSSQL
                Task mssqlTask = Task.Run(async () => mssqlResult = await this._mssql.UpdatePriceRecoveryAll(ePR, true));

                // update vao ORACLE
                Task oracleTask = Task.Run(async () => oracleResult = await this._oracle.UpdatePriceRecoveryAll(ePR, true));

                // wait all
                await Task.WhenAll(mssqlTask, oracleTask);

                // return data
                return new EBulkScript()
                {
                    MssqlScript = mssqlResult.Data.ToString(),
                    OracleScript = oracleResult.Data.ToString()
                };
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                // return null
                return null;
            }
        }

    }
}

