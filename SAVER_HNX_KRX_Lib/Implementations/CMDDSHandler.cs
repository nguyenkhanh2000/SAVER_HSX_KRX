using CommonLib.Interfaces;
using MDDSCore.Messages;
using Newtonsoft.Json;
using BaseSaverLib.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SystemCore.Entities;
using SystemCore.Temporaries;
using SystemCore.Interfaces;
using CommonLib.Implementations;
using PriceLib;
using System.Globalization;
using System.Diagnostics;
using StackExchange.Redis;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Newtonsoft.Json.Linq;
using static App.Metrics.Formatters.Json.TimerMetric;
using System.Text.RegularExpressions;
using static App.Metrics.Formatters.Json.BucketTimerMetric;
using StockCore.TAChart.Entities;

namespace BaseSaverLib.Implementations
{
    public class CMDDSHandler : IMDDSHandler
    {
        //Stopwatch m_SW_INSERTDB = new Stopwatch();// Create new stopwatch.
        //Stopwatch m_SW_RD = new Stopwatch();
        Stopwatch m_SW = new Stopwatch();
        // vars
        private readonly IS6GApp _app;
        private readonly IMDDSRepository _repository;
        private readonly CMonitor _monitor;
        private object m_objLocker = new object();
        private const string TEMPLATE_REDIS_KEY_REALTIME = "REALTIME:S5G_(Symbol)"; //   REALTIME:S5G_ABT
        public const int intPeriod = 43830; //đủ time cho key sống 1 tháng
        //KL theo thời gian lô chẵn
        private const string TEMPLATE_REDIS_KEY_LE = "LE:S5G_(Symbol)";       //   LE:S5G_ABT
        private const string TEMPLATE_JSONC_LE = "{\"MT\":\"(MT)\",\"MQ\":(MQ),\"MP\":(MP),\"TQ\":(TQ)}";

        // lô lẻ hsx
        public const string TEMPLATE_JSONC_PO = "{\"T\":\"(T)\",\"S\":\"(S)\",\"BP1\":(BP1),\"BQ1\":(BQ1),\"BP2\":(BP2),\"BQ2\":(BQ2),\"BP3\":(BP3),\"BQ3\":(BQ3),\"SP1\":(SP1),\"SQ1\":(SQ1),\"SP2\":(SP2),\"SQ2\":(SQ2),\"SP3\":(SP3),\"SQ3\":(SQ3)}";    //
        public const string TEMPLATE_REDIS_KEY_PO = "PO:S5G_(Symbol)";
        public const string TEMPLATE_REDIS_KEY_STOCK_NO_HNX = "Key_StockNo_Saver_HNX";
        public const string TEMPLATE_REDIS_KEY_STOCK_NO_HSX = "Key_StockNo_Saver_HSX";
        private const string TEMPLATE_REDIS_VALUE = "{\"Time\":\"(Now)\",\"Data\":[(RedisData)]}";

        private readonly CRedisConfig _redisConfig;
        private readonly CRedis_New _redis;
        private Dictionary<string, string> d_dic_stockno = new Dictionary<string, string>();//dic lưu stock no của mess d
        public TimeZoneInfo timeZone = TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time"); // UTC+7
        /// <summary>
        /// 2020-07-30 13:39:41 ngocta2
        /// constructor
        /// </summary>
        /// <param name="app"></param>
        /// <param name="repository"></param>
        public CMDDSHandler(IS6GApp app, IMDDSRepository repository, CRedisConfig redisConfig,CRedis_New redis, CMonitor monitor)
        {
            this._app = app;
            this._repository = repository;
            this._redisConfig = redisConfig;
            this._redis = redis;
           this._monitor = monitor;
        }
        public string GetMsgTypeSAN(string rawData)
        {
            string msgType = Regex.Match(rawData, "30001=(.*?)", RegexOptions.Multiline).Groups[1].Value;
            return msgType;
        }
        public bool BuildScriptSQL(string[] arrMsg)
        {
            var SW_RD = Stopwatch.StartNew();
            StringBuilder mssqlBuilder = new StringBuilder(EGlobalConfig.__STRING_SQL_BEGIN_TRANSACTION);
            StringBuilder oracleBuilder = new StringBuilder(EGlobalConfig.__STRING_ORACLE_BLOCK_BEGIN);
            EBulkScript eBulkScript = new EBulkScript();
            int count = 0;
            var stateRedis = new ProcessStateRedis();
            try
            {
                foreach(string dataMsg in arrMsg)
                {
                    count++;
                    string msgType = this._app.Common.GetMsgType(dataMsg);
                    //eBulkScript = await ProcessMessage(msgType, dataBlock);
                    eBulkScript = ProcessMessage(msgType, dataMsg, stateRedis).GetAwaiter().GetResult();
                    if (!string.IsNullOrEmpty(eBulkScript.MssqlScript))
                        mssqlBuilder.Append(EGlobalConfig.__STRING_RETURN_NEW_LINE + EGlobalConfig.__STRING_EXEC + EGlobalConfig.__STRING_SPACE + eBulkScript.MssqlScript).ToString();
                    if (!string.IsNullOrEmpty(eBulkScript.OracleScript))
                        oracleBuilder.Append(EGlobalConfig.__STRING_RETURN_NEW_LINE + EGlobalConfig.__STRING_TAB + EGlobalConfig.__STRING_SPACE + eBulkScript.OracleScript).ToString();                            
                }
                if(stateRedis.TotalCountArrMsg > 0)
                {
                    this._monitor.SendStatusToMonitor(this._app.Common.GetLocalDateTime(), this._app.Common.GetLocalIp(), CMonitor.MONITOR_APP.HSX_Feeder5G_Q, stateRedis.TotalCountArrMsg, stateRedis.StopwatchRD);
                }
                // them footer cho oracle script
                oracleBuilder.Append(EGlobalConfig.__STRING_ORACLE_BLOCK_END);
                // them footer cho mssql script
                mssqlBuilder.Append(EGlobalConfig.__STRING_RETURN_NEW_LINE + EGlobalConfig.__STRING_SQL_COMMIT_TRANSACTION);
                if ((!string.IsNullOrEmpty(eBulkScript.MssqlScript) && eBulkScript.MssqlScript.Length > 10) || (!string.IsNullOrEmpty(eBulkScript.OracleScript) && eBulkScript.OracleScript.Length > 10))
                {
                    if (!string.IsNullOrEmpty(eBulkScript.OracleScript))
                    {
                        
                        // exec script oracle
                        this._repository.ExecBulkScript(mssqlBuilder.ToString(), oracleBuilder.ToString());

                        this._monitor.SendStatusToMonitor(this._app.Common.GetLocalDateTime(), this._app.Common.GetLocalIp(), CMonitor.MONITOR_APP.HSX_Saver5G, count, SW_RD.ElapsedMilliseconds);
                        this._app.SqlLogger.LogSql(mssqlBuilder.ToString());
                    }
                    else
                    {
                        // exec script mssql
                        this._repository.ExecBulkScript(mssqlBuilder.ToString());

                        this._monitor.SendStatusToMonitor(this._app.Common.GetLocalDateTime(), this._app.Common.GetLocalIp(), CMonitor.MONITOR_APP.HSX_Saver5G, count, SW_RD.ElapsedMilliseconds);
                        this._app.SqlLogger.LogSql(mssqlBuilder.ToString());
                    }


                }
                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
            
        }

        /// <summary>
        /// 2020-08-19 09:23:03 ngocta2
        /// giam lap code, ko tot
        /// chu y: bo keyword async tai day
        /// https://stackoverflow.com/questions/29923215/should-i-worry-about-this-async-method-lacks-await-operators-and-will-run-syn
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rawData"></param>
        /// <returns></returns>
        public Task<T> Raw2Entity<T>(string rawData) where T : EBase
        {
            try
            {
                string json = this._app.Common.Fix2Json(rawData);
                T entity = JsonConvert.DeserializeObject<T>(json);
                entity.SendingTime = this._app.Common.FixToDateTimeString(TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(entity.SendingTime, "yyyyMMdd HH:mm:ss.fff", null), timeZone).ToString("yyyyMMdd HH:mm:ss.fff")); // can phai chuyen SendingTime tu string sang DateTime
                //entity.SendingTime = this._app.Common.FixToDateTimeString(entity.SendingTime.ToString());
                return Task.FromResult(entity);
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogError(ex);
                return null;
            }
        }

        /// <summary>
        /// 2020-07-31 14:44:33 ngocta2
        /// xu ly data : convert raw data thanh obj, pass vao DAL >> can lam song song
        /// </summary>
        /// <param name="msgType"></param>
        /// <param name="rawData"></param>
        /// <returns></returns>
        public async Task<EBulkScript> ProcessMessage(string msgType, string rawData, ProcessStateRedis _state)
        {
            TExecutionContext ec = this._app.DebugLogger.WriteBufferBegin($"ProcessMessage msgType={msgType}; rawData={rawData}", true);
            try
            {
                EBulkScript eBulkScript = new EBulkScript();
                switch (msgType)
                {
                    // 4.1 - Security Definition
                    case ESecurityDefinition.__MSG_TYPE:
                        ESecurityDefinition eSD = await this.Raw2Entity<ESecurityDefinition>(rawData);

                        // Lấy ra key msg lưu db
                        if ((eSD.MarketID == "STO") && eSD.BoardID == "G1" && !d_dic_stockno.ContainsKey(eSD.Symbol))
                        {
                            d_dic_stockno[eSD.Symbol] = eSD.TickerCode;
                            string stockno = JsonConvert.SerializeObject(d_dic_stockno);
                            _redis.SetCacheBI(TEMPLATE_REDIS_KEY_STOCK_NO_HSX, stockno, intPeriod);
                        }else if ((eSD.MarketID == "STX" || eSD.MarketID == "UPX" || eSD.MarketID == "DVX") && eSD.BoardID == "G1" && !d_dic_stockno.ContainsKey(eSD.Symbol))
                        {
                            d_dic_stockno[eSD.Symbol] = eSD.TickerCode;
                            string stockno = JsonConvert.SerializeObject(d_dic_stockno);
                            _redis.SetCache(TEMPLATE_REDIS_KEY_STOCK_NO_HNX, stockno, intPeriod);
                        }
                        eBulkScript = await _repository.GetScriptSecurityDefinition(eSD);

                        break;
                    // 4.2 - Security Status
                    case ESecurityStatus.__MSG_TYPE:
                        ESecurityStatus eSS = await this.Raw2Entity<ESecurityStatus>(rawData);
                        eBulkScript = await _repository.GetScriptSecurityStatus(eSS);
                        break;
                    // 4.3 - Security Information Notification
                    case ESecurityInformationNotification.__MSG_TYPE:
                        ESecurityInformationNotification eSIN = await this.Raw2Entity<ESecurityInformationNotification>(rawData);
                        eBulkScript = await _repository.GetScriptSecurityInformationNotification(eSIN);
                        break;
                    // 4.4 - Symbol Closing Information
                    case ESymbolClosingInformation.__MSG_TYPE:
                        ESymbolClosingInformation eSCI = await this.Raw2Entity<ESymbolClosingInformation>(rawData);
                        eBulkScript = await _repository.GetScriptSymbolClosingInformation(eSCI);
                        break;
                    // 4.5 - Volatility Interruption
                    case EVolatilityInterruption.__MSG_TYPE:
                        EVolatilityInterruption eVI = await this.Raw2Entity<EVolatilityInterruption>(rawData);
                        eBulkScript = await _repository.GetScriptVolatilityInterruption(eVI);
                        break;
                    // 4.6 - Market Maker Information
                    case EMarketMakerInformation.__MSG_TYPE:
                        EMarketMakerInformation eMMI = await this.Raw2Entity<EMarketMakerInformation>(rawData);
                        eBulkScript = await _repository.GetScriptMarketMakerInformation(eMMI);
                        break;
                    // 4.7 - Symbol Event
                    case ESymbolEvent.__MSG_TYPE:
                        ESymbolEvent eSE = await this.Raw2Entity<ESymbolEvent>(rawData);
                        eSE.EventStartDate = this._app.Common.FixToDateString(eSE.EventStartDate.ToString());
                        eSE.EventEndDate = this._app.Common.FixToDateString(eSE.EventEndDate.ToString());// can phai chuyen SendingTime tu string sang DateTime											              
                        eBulkScript = await _repository.GetScriptSymbolEvent(eSE);
                        break;
                    // 4.8 - Index Constituents Information
                    case EIndexConstituentsInformation.__MSG_TYPE:
                        EIndexConstituentsInformation eICI = await this.Raw2Entity<EIndexConstituentsInformation>(rawData);
                        eBulkScript = await _repository.GetScriptIndexConstituentsInformation(eICI);
                        break;
                    // 4.9 - Random End
                    case ERandomEnd.__MSG_TYPE:
                        ERandomEnd eRE = await this.Raw2Entity<ERandomEnd>(rawData);
                        eRE.TransactTime = this._app.Common.FixToTimeString(eRE.TransactTime.ToString());// can phai chuyen SendingTime tu string sang DateTime											              
                        eBulkScript = await _repository.GetScriptRandomEnd(eRE);
                        break;
                    // 4.10 Price
                    case EPrice.__MSG_TYPE:
                        EPrice eP = this._app.HandCode.Fix_Fix2EPrice(rawData, true,1,2,1);
                        var stopWatch = Stopwatch.StartNew();
                        //Update key giá khớp lệnh-- hiển thị cho phần chi tiết giá
                        if ((eP.MarketID == "STO" || eP.MarketID == "STX" || eP.MarketID == "UPX" || eP.MarketID == "DVX") && eP.BoardID == "G1" && eP.Side == null)
                        {
                            await UpdateRedisLE(eP);
                            _state.TotalCountArrMsg++;
                        }
                        else if ((eP.MarketID == "STO" || eP.MarketID == "STX" || eP.MarketID == "UPX" || eP.MarketID == "DVX") && eP.BoardID == "G4" && eP.Side != null)
                        {
                            // Giao dịch lô lẻ cho phần chi tiết giá
                            await UpdateRedisPO(eP);
                            _state.TotalCountArrMsg++;
                        }
                        _state.StopwatchRD += stopWatch.ElapsedMilliseconds;
                        eBulkScript = await _repository.GetScriptPriceAll(eP);
                        break;
                    // 4.11 Price Recovery
                    case EPriceRecovery.__MSG_TYPE:
                        EPriceRecovery ePR = this._app.HandCode.Fix_Fix2EPriceRecovery(rawData, true,1,2,1);
                        eBulkScript = await _repository.GetScriptPriceRecoveryAll(ePR);
                        break;
                    // 4.13 - Index
                    case EIndex.__MSG_TYPE:
                        EIndex eI = await this.Raw2Entity<EIndex>(rawData);
                        eI.TransDate = this._app.Common.FixToTransDateString(eI.SendingTime.ToString());
                        eI.TransactTime = this._app.Common.FixToTimeString(eI.TransactTime.ToString());// can phai chuyen SendingTime tu string sang DateTime											              
                        eBulkScript = await _repository.GetScriptIndex(eI);
                        break;
                    // 4.14 - Investor per Industry
                    case EInvestorPerIndustry.__MSG_TYPE:
                        EInvestorPerIndustry eIPI = await this.Raw2Entity<EInvestorPerIndustry>(rawData);
                        eIPI.TransactTime = this._app.Common.FixToTimeString(eIPI.TransactTime.ToString());// can phai chuyen SendingTime tu string sang DateTime											              
                        eBulkScript = await _repository.GetScriptInvestorperIndustry(eIPI);
                        break;

                    // 4.17 - Investor per Symbol
                    case EInvestorPerSymbol.__MSG_TYPE:
                        EInvestorPerSymbol eIPS = await this.Raw2Entity<EInvestorPerSymbol>(rawData);
                        eBulkScript = await _repository.GetScriptInvestorperSymbol(eIPS);
                        break;
                    // 4.18 - Top N Members per Symbol
                    case ETopNMembersPerSymbol.__MSG_TYPE:
                        ETopNMembersPerSymbol eTNMPS = await this.Raw2Entity<ETopNMembersPerSymbol>(rawData);
                        eBulkScript = await _repository.GetScriptTopNMembersperSymbol(eTNMPS);
                        break;
                    // 4.19 - Open Interest
                    case EOpenInterest.__MSG_TYPE:
                        EOpenInterest eOI = await this.Raw2Entity<EOpenInterest>(rawData);
                        eOI.TradeDate = this._app.Common.FixToDateString(eOI.TradeDate.ToString());
                        eBulkScript = await _repository.GetScriptOpenInterest(eOI);
                        break;
                    // 4.20 - Deem Trade Price
                    case EDeemTradePrice.__MSG_TYPE:
                        EDeemTradePrice eDTP = await this.Raw2Entity<EDeemTradePrice>(rawData);
                        eBulkScript = await _repository.GetScriptDeemTradePrice(eDTP);
                        break;
                    // 4.21 - Foreigner Order Limit
                    case EForeignerOrderLimit.__MSG_TYPE:
                        EForeignerOrderLimit eFOL = await this.Raw2Entity<EForeignerOrderLimit>(rawData);
                        eBulkScript = await _repository.GetScriptForeignerOrderLimit(eFOL);
                        break;
                    // 4.22 - Price Limit Expansion
                    case EPriceLimitExpansion.__MSG_TYPE:
                        EPriceLimitExpansion ePLE = await this.Raw2Entity<EPriceLimitExpansion>(rawData);
                        eBulkScript = await _repository.GetScriptPriceLimitExpansion(ePLE);
                        break;
                    // 4.23 - EETF iNav
                    case EETFiNav.__MSG_TYPE:
                        EETFiNav eEiN = await this.Raw2Entity<EETFiNav>(rawData);
                        eBulkScript = await _repository.GetScriptETFiNav(eEiN);
                        break;
                    // 4.24 - EETF iIndex
                    case EETFiIndex.__MSG_TYPE:
                        EETFiIndex eEiI = await this.Raw2Entity<EETFiIndex>(rawData);
                        eBulkScript = await _repository.GetScriptETFiIndex(eEiI);
                        break;
                    // 4.25 - EETF TrackingError
                    case EETFTrackingError.__MSG_TYPE:
                        EETFTrackingError eETE = await this.Raw2Entity<EETFTrackingError>(rawData);
                        eETE.TradeDate = this._app.Common.FixToDateString(eETE.TradeDate.ToString());
                        eBulkScript = await _repository.GetScriptETFTrackingError(eETE);
                        break;
                    // 4.26 - Top N Symbols with Trading Quantity
                    case ETopNSymbolsWithTradingQuantity.__MSG_TYPE:
                        ETopNSymbolsWithTradingQuantity ETNSWTQ = await this.Raw2Entity<ETopNSymbolsWithTradingQuantity>(rawData);
                        eBulkScript = await _repository.GetScriptTopNSymbolswithTradingQuantity(ETNSWTQ);
                        break;
                    // 4.27 - Top N Symbols with  Current Price
                    case ETopNSymbolsWithCurrentPrice.__MSG_TYPE:
                        ETopNSymbolsWithCurrentPrice ETNSWCP = await this.Raw2Entity<ETopNSymbolsWithCurrentPrice>(rawData);
                        eBulkScript = await _repository.GetScriptTopNSymbolswithCurrentPrice(ETNSWCP);
                        break;
                    // 4.28 - Top N Symbols with High Ratio of Price
                    case ETopNSymbolsWithHighRatioOfPrice.__MSG_TYPE:
                        ETopNSymbolsWithHighRatioOfPrice ETNSWHROP = await this.Raw2Entity<ETopNSymbolsWithHighRatioOfPrice>(rawData);
                        eBulkScript = await _repository.GetScriptTopNSymbolswithHighRatioofPrice(ETNSWHROP);
                        break;

                    // 4.29 - Top N Symbols with Low Ratio of Price
                    case ETopNSymbolsWithLowRatioOfPrice.__MSG_TYPE:
                        ETopNSymbolsWithLowRatioOfPrice ETNSWLROP = await this.Raw2Entity<ETopNSymbolsWithLowRatioOfPrice>(rawData);
                        eBulkScript = await _repository.GetScriptTopNSymbolswithLowRatioofPrice(ETNSWLROP);
                        break;
                    // 4.30 - Trading Result of Foreign Investors
                    case ETradingResultOfForeignInvestors.__MSG_TYPE:
                        ETradingResultOfForeignInvestors ETRFI = await this.Raw2Entity<ETradingResultOfForeignInvestors>(rawData);
                        if (ETRFI.TransactTime != null)
                        {
                            ETRFI.TransactTime = this._app.Common.FixToTimeString(ETRFI.TransactTime.ToString());// can phai chuyen SendingTime tu string sang DateTime	
                        }

                        eBulkScript = await _repository.GetScriptTradingResultofForeignInvestors(ETRFI);
                        break;
                    // 4.31 - Disclosure
                    case EDisclosure.__MSG_TYPE:
                        EDisclosure eD = await this.Raw2Entity<EDisclosure>(rawData);
                        eD.PublicInformationDate = this._app.Common.FixToDateString(eD.PublicInformationDate.ToString());
                        eD.TransmissionDate = this._app.Common.FixToDateString(eD.TransmissionDate.ToString());// can phai chuyen SendingTime tu string sang DateTime		
                        eBulkScript = await _repository.GetScriptDisclosure(eD);
                        break;
                    // 4.32 - TimeStampPolling
                    //case ETimeStampPolling.__MSG_TYPE:
                    //    ETimeStampPolling eTSP = await this.Raw2Entity<ETimeStampPolling>(rawData);
                    //    eTSP.TransactTime = this._app.Common.FixToTimeString(eTSP.TransactTime.ToString());// can phai chuyen SendingTime tu string sang DateTime		
                    //    eBulkScript = await _repository.GetScriptTimeStampPolling(eTSP);
                    //    break;
                    // 4.33 - DrvProductEvent
                    case EDrvProductEvent.__MSG_TYPE:
                        EDrvProductEvent eDRV = await this.Raw2Entity<EDrvProductEvent>(rawData);
                        //  eDRV.PublicInformationDate = this._app.Common.FixToDateString(eDRV.PublicInformationDate.ToString());
                        // eDRV.TransmissionDate = this._app.Common.FixToDateString(eDRV.TransmissionDate.ToString());// can phai chuyen SendingTime tu string sang DateTime		
                        eBulkScript = await _repository.GetScriptDrvProductEvent(eDRV);
                        break;
                }

                return eBulkScript;
            }
            catch (Exception ex)
            {
                // log error + buffer data
                this._app.ErrorLogger.LogErrorContext(ex, ec);
                return null;
            }
        }
        public async Task UpdateRedisLE(EPrice eP)
        {
            try
            {
                string Symbol = "";
                string value = "";
                var stopWatch = Stopwatch.StartNew();
                //var SW = Stopwatch.StartNew();
                if (d_dic_stockno.Count < 1)
                {
                    if (eP.MarketID == "STO")
                    {
                        value = _redis.RC_1.StringGet(TEMPLATE_REDIS_KEY_STOCK_NO_HSX);
                    }
                    else if (eP.MarketID == "STX" || eP.MarketID == "UPX" || eP.MarketID == "DVX")
                    {
                        value = _redis.RC_1.StringGet(TEMPLATE_REDIS_KEY_STOCK_NO_HNX);
                    }

                    if (!string.IsNullOrEmpty(value))
                    {

                        Dictionary<string, string> storedDictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(value);

                        foreach (var kew in storedDictionary)
                        {
                            if (d_dic_stockno.ContainsKey(kew.Key))
                            {
                                d_dic_stockno[kew.Key] = kew.Value;
                            }
                            else
                            {
                                d_dic_stockno.Add(kew.Key, kew.Value);
                            }
                        }
                    }
                }
                if (d_dic_stockno.ContainsKey(eP.Symbol))
                {
                    Symbol = d_dic_stockno[eP.Symbol];
                    string time = (eP.SendingTime.Split(' ')[1]).Split('.')[0];
                    string strJsonC = "";
                    if (eP.MarketID == "STO")
                    {
                        strJsonC = TEMPLATE_JSONC_LE
                                .Replace("(MT)", time.ToString())
                                .Replace("(MQ)", Processkl(eP.MatchQuantity).ToString())
                                .Replace("(MP)", ProcessPrice(eP.MatchPrice).ToString())
                                .Replace("(TQ)", Processkl(eP.TotalVolumeTraded).ToString())
                                ;
                        //this._monitor.SendStatusToMonitor(this._app.Common.GetLocalDateTime(), this._app.Common.GetLocalIp(), CMonitor.MONITOR_APP.HSX_Feeder5G_Q, strJsonC.Length, stopWatch.ElapsedMilliseconds);
                    }
                    else
                    {
                        strJsonC = TEMPLATE_JSONC_LE
                                .Replace("(MT)", time.ToString())
                                .Replace("(MQ)", eP.MatchQuantity.ToString())
                                .Replace("(MP)", ProcessPrice(eP.MatchPrice).ToString())
                                .Replace("(TQ)", eP.TotalVolumeTraded.ToString())
                                ;
                        //this._monitor.SendStatusToMonitor(this._app.Common.GetLocalDateTime(), this._app.Common.GetLocalIp(), CMonitor.MONITOR_APP.HSX_Feeder5G_Q, strJsonC.Length, stopWatch.ElapsedMilliseconds);
                    }
                    string Z_KEY = TEMPLATE_REDIS_KEY_LE.Replace("(Symbol)", Symbol);
                    long Z_SCORE = Convert.ToInt64(DateTime.Now.ToString("yyyyMMddHHmmssfff"));
                    string Z_VALUE = strJsonC;

                    await this._redis.SortedSetAddAsync(Z_KEY, Z_VALUE, Z_SCORE);
                }
            }
            catch (Exception ex) 
            {
                this._app.ErrorLogger.LogError(ex);
            }
        }
        public async Task UpdateRedisPO(EPrice eP)
        {
            try
            {
                string Symbol = "";
                string value = "";
                var stopWatch = Stopwatch.StartNew();
                if (d_dic_stockno.Count < 1)
                {
                    if (eP.MarketID == "STO")
                    {
                        value = _redis.RC_1.StringGet(TEMPLATE_REDIS_KEY_STOCK_NO_HSX);
                    }
                    else if (eP.MarketID == "STX" || eP.MarketID == "UPX" || eP.MarketID == "DVX")
                    {
                        value = _redis.RC_1.StringGet(TEMPLATE_REDIS_KEY_STOCK_NO_HNX);
                    }
                    if (!string.IsNullOrEmpty(value))
                    {
                        Dictionary<string, string> storedDictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(value);

                        foreach (var kew in storedDictionary)
                        {
                            if (d_dic_stockno.ContainsKey(kew.Key))
                            {
                                d_dic_stockno[kew.Key] = kew.Value;
                            }
                            else
                            {
                                d_dic_stockno.Add(kew.Key, kew.Value);
                            }
                        }
                    }
                }
                if (d_dic_stockno.ContainsKey(eP.Symbol))
                {
                    Symbol = d_dic_stockno[eP.Symbol];
                    string sbJsonC = TEMPLATE_JSONC_PO
                                        .Replace("(T)", eP.SendingTime.Replace(" ", "-").Substring(0, eP.SendingTime.IndexOf(".")))
                                        .Replace("(S)", Symbol)
                                        .Replace("(BP1)", ProcessPrice(eP.BuyPrice1).ToString())
                                        .Replace("(BQ1)", eP.BuyQuantity1.ToString())
                                        .Replace("(BP2)", ProcessPrice(eP.BuyPrice2).ToString())
                                        .Replace("(BQ2)", eP.BuyQuantity2.ToString())
                                        .Replace("(BP3)", ProcessPrice(eP.BuyPrice3).ToString())
                                        .Replace("(BQ3)", eP.BuyQuantity3.ToString())
                                        .Replace("(SP1)", ProcessPrice(eP.SellPrice1).ToString())
                                        .Replace("(SQ1)", eP.SellQuantity1.ToString())
                                        .Replace("(SP2)", ProcessPrice(eP.SellPrice2).ToString())
                                        .Replace("(SQ2)", eP.SellQuantity2.ToString())
                                        .Replace("(SP3)", ProcessPrice(eP.SellPrice3).ToString())
                                        .Replace("(SQ3)", eP.SellQuantity3.ToString());

                    string Z_KEY = TEMPLATE_REDIS_KEY_PO
                            .Replace("(Symbol)", Symbol);

                    long Z_SCORE = Convert.ToInt64(DateTime.Now.ToString("yyyyMMddHHmmssfff"));
                    string Z_VALUE = TEMPLATE_REDIS_VALUE.Replace("(Now)", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"))
                                         .Replace("(RedisData)", sbJsonC);

                    await this._redis.SortedSetAddAsync(Z_KEY, Z_VALUE, intPeriod);

                    //this._monitor.SendStatusToMonitor(this._app.Common.GetLocalDateTime(), this._app.Common.GetLocalIp(), CMonitor.MONITOR_APP.HSX_Feeder5G_Q, sbJsonC.Length, stopWatch.ElapsedMilliseconds);
                }
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
            }
        }

        public double ProcessPrice(double priceString, int priceDividedBy = 1000, int priceRoundDigitsCount = 2)
        {
            double price = priceString; // 43100
            price = price / priceDividedBy; // 43.1
            price = Math.Round(price, priceRoundDigitsCount); // 43.1
            return price;
        }

        public int Processkl(long priceString, int priceDividedBy = 10)
        {
            int kl = Convert.ToInt32(priceString); // 43100
            kl = kl / priceDividedBy; // 43.1
            return kl;
        }

    }
    public class ProcessStateRedis
    {
        public int TotalCountArrMsg { get; set; }
        public long StopwatchRD { get; set; }
    }
}
