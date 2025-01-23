using CommonLib.Interfaces;
using MDDSCore.Messages;
using Microsoft.AspNetCore.Http;
using StockCore.Redis.Entities;
using StockCore.Stock6G.Entities;
using StockCore.Stock6G.JsonX;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using SystemCore.Entities;
using SystemCore.SharedKernel;
using Utf8Json;
using Utf8Json.Resolvers;

namespace CommonLib.Implementations
{
    /// <summary>
    /// 2020-08-21 15:03:45 ngocta2
    /// 2020-09-03 14:46:29 ngocta2
    /// Hand Coding : viet code that su dai dong hon cach dung cac frame/lib co san nhung co the dat duoc speed, toc do xu ly nhanh nhat
    /// + multithread/parallel/task
    /// + giam toi da cac log debug ko can thiet
    /// + bo cac frame/lib high level de viet hand code 
    /// + su dung cac tip/trick tang toc 
    /// + performance test nhieu lan de tim ra cach nhanh nhat
    /// https://peterdaugaardrasmussen.com/2018/10/02/c-run-each-item-in-a-list-in-parallel-and-wait-for-them-all/
    /// </summary>
    public class CHandCode : CInstance, IHandCode
    {

		// const 
		const string __STRING_EQUAL = EGlobalConfig.__STRING_EQUAL; //  ;"=";
		const string __STRING_FIX_SEPARATOR = EBasePrice.__STRING_FIX_SEPARATOR;//  "";
		const string __STRING_FIX_TAG_MSG_TYPE = EBasePrice.__TAG_35;               // "35";
                                                                                    //const int __UNIT_INDEX_VALUE = 100;// so tra ve 12345 => nhung so that phai la 123.45
        TimeZoneInfo timeZone = TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time"); // UTC+7
    
        /// <summary>
        /// constructor
        /// </summary>
        public CHandCode()
        {

        }
        public ESecurityStatusConvert arraySC;
        /// <summary>
        /// 2020-09-03 16:35:16 ngocta2
        /// lay chinh xac ky tu sau tag 35
        /// </summary>
        /// <param name="rawData">"8=FIX.4.49=52035=d49=VNMGW56=9999934=152=20190517 09:13:04.84430001=BDO20004=G1911=105207=HO55=VN0ANC11601630624=ANC1160130628=2330629=CTY DINH DUONG NN QUOC TE30630=CTY DINH DUONG NN QUOC TE20009=B1BDOBS20003=BDO30604=BS201=1194=541=106=ID00000130225=20171207231=1.0223=3.015=VND20020=30000001149=119103.01148=103521.0202=0.0965=N30631=1.01193=236=0.020013=111312.020014=0.020015=0.020016=0.0140=111312.020027=330642=2018120530511=30301=2016121930614=120018=30625=0.030635=30636=30637=10=239"</param>
        /// <returns>d</returns>
        public string Fix_GetMsgType(string rawData)
        {
            //int p   = rawData.IndexOf(__STRING_FIX_SEPARATOR + __STRING_FIX_TAG_MSG_TYPE);
            //rawData = rawData.Substring(p + 2 + __STRING_FIX_TAG_MSG_TYPE.Length);
            //p       = rawData.IndexOf(__STRING_FIX_SEPARATOR);
            //string msgType = rawData.Substring(0, p);
            //return msgType;
            //string msgType = Regex.Match(rawData, "35=(.*?)", RegexOptions.Multiline).Groups[1].Value;
            string msgType = Regex.Match(rawData, @"(?<=" + __STRING_FIX_SEPARATOR + __STRING_FIX_TAG_MSG_TYPE + "=)(.*?)(?=" + __STRING_FIX_SEPARATOR + ")").ToString();

            return msgType;
        }

        public string Fix_GetMsgTypes(string rawData)
        {
            //int p   = rawData.IndexOf(__STRING_FIX_SEPARATOR + __STRING_FIX_TAG_MSG_TYPE);
            //rawData = rawData.Substring(p + 2 + __STRING_FIX_TAG_MSG_TYPE.Length);
            //p       = rawData.IndexOf(__STRING_FIX_SEPARATOR);
            //string msgType = rawData.Substring(0, p);
            //return msgType;
            string msgType = Regex.Match(rawData, "30001=(.*?)", RegexOptions.Multiline).Groups[1].Value;
            return msgType;
        }
        /// <summary>
        /// 2020-08-21 15:07:38 ngocta2
        /// chuyen raw data (fix format) sang object ESecurityDefinition (d)
        /// chi giu lai cac field can thiet, ko can thi bo het (comment)
        /// </summary>
        /// <param name="rawData">"8=FIX.4.49=52035=d49=VNMGW56=9999934=152=20190517 09:13:04.84430001=BDO20004=G1911=105207=HO55=VN0ANC11601630624=ANC1160130628=2330629=CTY DINH DUONG NN QUOC TE30630=CTY DINH DUONG NN QUOC TE20009=B1BDOBS20003=BDO30604=BS201=1194=541=106=ID00000130225=20171207231=1.0223=3.015=VND20020=30000001149=119103.01148=103521.0202=0.0965=N30631=1.01193=236=0.020013=111312.020014=0.020015=0.020016=0.0140=111312.020027=330642=2018120530511=30301=2016121930614=120018=30625=0.030635=30636=30637=10=239"</param>
        /// <returns>1000</returns>
        public ESecurityDefinition Fix_Fix2ESecurityDefinition(string rawData, int priceDividedBy = 10, int priceRoundDigitsCount = 2)
        {
            ESecurityDefinition eSD = new ESecurityDefinition();
            string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            foreach (string pair in arr)
            {
                if (!string.IsNullOrEmpty(pair))
                {
                    string[] parts = pair.Split(__STRING_EQUAL);
                    switch (parts[0])
                    {
						case EBase.__TAG_8    : eSD.BeginString                      = parts[1];                   break;
						case EBase.__TAG_9    : eSD.BodyLength                       = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_35   : eSD.MsgType                          = parts[1];                   break;
						case EBase.__TAG_49   : eSD.SenderCompID                     = parts[1];                   break;
						case EBase.__TAG_56   : eSD.TargetCompID                     = parts[1];                   break;
						case EBase.__TAG_34     : eSD.MsgSeqNum                        = Convert.ToInt64(parts[1]);  break;
                        case EBase.__TAG_52: eSD.SendingTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(parts[1], "yyyyMMdd HH:mm:ss.fff", null), timeZone).ToString("yyyyMMdd HH:mm:ss.fff"); break;
                        ////===============================================================================================
						case EBase.__TAG_30001	: eSD.MarketID                         = parts[1];                   break;
						case EBase.__TAG_20004  : eSD.BoardID                          = parts[1];                   break;
						case EBase.__TAG_911  : eSD.TotNumReports                    = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_207  : eSD.SecurityExchange                 = parts[1];                   break;
						case EBase.__TAG_55		: eSD.Symbol                           = parts[1];                   break;
						case EBase.__TAG_30624	: eSD.TickerCode                       = parts[1];                   break;
						case EBase.__TAG_30628  : eSD.SymbolShortCode                  = parts[1];                   break;
						case EBase.__TAG_30629: eSD.SymbolName                       = parts[1];                   break;
						case EBase.__TAG_30630: eSD.SymbolEnglishName                = parts[1];                   break;
						case EBase.__TAG_20009: eSD.ProductID                        = parts[1];                   break;
						case EBase.__TAG_20003: eSD.ProductGrpID                     = parts[1];                   break;
						case EBase.__TAG_30604: eSD.SecurityGroupID                  = parts[1];                   break;
						case EBase.__TAG_201  : eSD.PutOrCall                        = parts[1];                   break;
						case EBase.__TAG_1194 : eSD.ExerciseStyle                    = parts[1];                   break;
						case EBase.__TAG_200  : eSD.MaturityMonthYear                = parts[1];                   break;
						case EBase.__TAG_541  : eSD.MaturityDate                     = parts[1];                   break;
						case EBase.__TAG_106  : eSD.Issuer                           = parts[1];                   break;
						case EBase.__TAG_225  : eSD.IssueDate                        = parts[1];                   break;
						case EBase.__TAG_231  : eSD.ContractMultiplier               = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_223  : eSD.CouponRate                       = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_15   : eSD.Currency                         = parts[1];                   break;
						case EBase.__TAG_20020: eSD.ListedShares                     = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_1149	: eSD.HighLimitPrice                   = this.ProcessPrice(parts[1], priceDividedBy, priceRoundDigitsCount); break;
						case EBase.__TAG_1148	: eSD.LowLimitPrice                    = this.ProcessPrice(parts[1], priceDividedBy, priceRoundDigitsCount); break;
						case EBase.__TAG_202  : eSD.StrikePrice                      = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_965  : eSD.SecurityStatus                   = parts[1];                   break;
						case EBase.__TAG_30631: eSD.ContractSize                     = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_1193 : eSD.SettlMethod                      = parts[1];                   break;
						case EBase.__TAG_236  : eSD.Yield                            = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_20013	: eSD.ReferencePrice                   = this.ProcessPrice(parts[1], priceDividedBy, priceRoundDigitsCount); break;
						case EBase.__TAG_20014: eSD.EvaluationPrice                  = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_20015: eSD.HgstOrderPrice                   = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_20016: eSD.LwstOrderPrice                   = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_140  : eSD.PrevClosePx                      = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_20027: eSD.SymbolCloseInfoPxType            = parts[1];                   break;
						case EBase.__TAG_30642: eSD.FirstTradingDate                 = parts[1];                   break;
						case EBase.__TAG_30511	: eSD.FinalTradeDate                   = parts[1];                   break;
						case EBase.__TAG_30512: eSD.FinalSettleDate                  = parts[1];                   break;
						case EBase.__TAG_30301: eSD.ListingDate                      = parts[1];                   break;
						//case EBase.__TAG_30540	: eSD.OpenInterestQty                  = Convert.ToInt64(parts[1]);  break;
						//case EBase.__TAG_30573: eSD.SettlementPrice                  = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_30614: eSD.RandomEndTriggeringConditionCode = parts[1];                   break;
						case EBase.__TAG_20018	: eSD.ExClassType                      = parts[1];                   break;
						case EBase.__TAG_30625	: eSD.VWAP                             = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_30635: eSD.SymbolAdminStatusCode            = parts[1];                   break;
						case EBase.__TAG_30636	: eSD.SymbolTradingMethodStatusCode    = parts[1];                   break;
						case EBase.__TAG_30637	: eSD.SymbolTradingSantionStatusCode   = parts[1];                   break;
                            ////====================================================================================
                            //case EBase.__TAG_10   : eSD.CheckSum                         = parts[1];                   break;
                    }
                }
            }

            return eSD;
        }

        /// <summary>
        /// 2020-09-09 16:12:26 ngocta2
        /// chuyen raw data (fix format) sang object ETradingResultOfForeignInvestors (MT)
        /// chi giu lai cac field can thiet, ko can thi bo het (comment)
        /// </summary>
        /// <param name="rawData"></param>
        /// <returns></returns>
        public ETradingResultOfForeignInvestors Fix_Fix2ETradingResultOfForeignInvestors(string rawData)
        {
            ETradingResultOfForeignInvestors eFI = new ETradingResultOfForeignInvestors();
            string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            foreach (string pair in arr)
            {
                if (!string.IsNullOrEmpty(pair))
                {
                    string[] parts = pair.Split(__STRING_EQUAL);
                    switch (parts[0])
                    {
                        //case EBase.__TAG_8     : eFI.BeginString          = parts[1];                   break;
						case EBase.__TAG_9     : eFI.BodyLength           = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_35    : eFI.MsgType              = parts[1];                   break;
                        //case EBase.__TAG_49    : eFI.SenderCompID         = parts[1];                   break;
                        //case EBase.__TAG_56    : eFI.TargetCompID         = parts[1];                   break;
						case EBase.__TAG_34      : eFI.MsgSeqNum            = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_52    : eFI.SendingTime            = parts[1];                   break;
                        ////====================================================================================
						case EBase.__TAG_30001   : eFI.MarketID             = parts[1];                   break;
						case EBase.__TAG_20004   : eFI.BoardID              = parts[1];                   break;
                        //case EBase.__TAG_336   : eFI.TradingSessionID     = parts[1];                   break;
						case EBase.__TAG_55      : eFI.Symbol               = parts[1];                   break;
                        //case EBase.__TAG_60    : eFI.TransactTime         = parts[1];                   break;
						case EBase.__TAG_20054 : eFI.FornInvestTypeCode   = parts[1];                   break;
						case EBase.__TAG_331	 : eFI.SellVolume = Convert.ToInt64(parts[1]); break;
						case EBase.__TAG_30168 : eFI.SellTradeAmount      = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_330	 : eFI.BuyVolume = Convert.ToInt64(parts[1]); break;
						case EBase.__TAG_30169 : eFI.BuyTradedAmount      = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_30643 : eFI.SellVolumeTotal      = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_30644 : eFI.SellTradeAmountTotal = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_30645 : eFI.BuyVolumeTotal       = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_30646 : eFI.BuyTradeAmountTotal  = Convert.ToDouble(parts[1]); break;
                            ////====================================================================================
                            //case EBase.__TAG_10    : eFI.CheckSum             = parts[1];                   break;
                    }
                }
            }

            return eFI;
        }


        /// <summary>
        /// 2020-09-09 16:12:26 ngocta2
        /// chuyen raw data (fix format) sang object EForeignerOrderLimit (MF)
        /// chi giu lai cac field can thiet, ko can thi bo het (comment)
        /// </summary>
        /// <param name="rawData"></param>
        /// <returns></returns>
        public EForeignerOrderLimit Fix_Fix2EForeignerOrderLimit(string rawData)
        {
            EForeignerOrderLimit eFOL = new EForeignerOrderLimit();
            string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            foreach (string pair in arr)
            {
                if (!string.IsNullOrEmpty(pair))
                {
                    string[] parts = pair.Split(__STRING_EQUAL);
                    switch (parts[0])
                    {
                        //case EBase.__TAG_8    : eFOL.BeginString            = parts[1];                  break;
						case EBase.__TAG_9    : eFOL.BodyLength               = Convert.ToInt64(parts[1]); break;
                        //case EBase.__TAG_35   : eFOL.MsgType                = parts[1];                  break;
                        //case EBase.__TAG_49   : eFOL.SenderCompID           = parts[1];                  break;
                        //case EBase.__TAG_56   : eFOL.TargetCompID           = parts[1];                  break;
						case EBase.__TAG_34     : eFOL.MsgSeqNum              = Convert.ToInt64(parts[1]); break;
						case EBase.__TAG_52   : eFOL.SendingTime              = parts[1];                  break;
                        ////====================================================================================
						case EBase.__TAG_30001	: eFOL.MarketID               = parts[1];                  break;
						case EBase.__TAG_55		: eFOL.Symbol                 = parts[1];                  break;
                        //case EBase.__TAG_30557: eFOL.ForeignerBuyPosblQty   = Convert.ToInt64(parts[1]); break;
						case EBase.__TAG_30558	: eFOL.ForeignerOrderLimitQty = Convert.ToInt64(parts[1]); break;
                            ////====================================================================================
                            //case EBase.__TAG_10   : eFOL.CheckSum               = parts[1];                  break;
                    }
                }
            }

            return eFOL;
        }

        /// <summary>
        /// 2020-09-09 16:12:26 ngocta2
        /// chuyen raw data (fix format) sang object EForeignerOrderLimit (MW)
        /// chi giu lai cac field can thiet, ko can thi bo het (comment)
        /// </summary>
        /// <param name="rawData"></param>
        /// <returns></returns>
        public ERandomEnd Fix_Fix2ERandomEnd(string rawData)
        {
            ERandomEnd eRE = new ERandomEnd();
            string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            foreach (string pair in arr)
            {
                if (!string.IsNullOrEmpty(pair))
                {
                    string[] parts = pair.Split(__STRING_EQUAL);
                    switch (parts[0])
                    {
                        //case EBase.__TAG_8    : eRE.BeginString                                  = parts[1];                   break;
					    case EBase.__TAG_9    : eRE.BodyLength                                   = Convert.ToInt64(parts[1]);  break;
                        //case EBase.__TAG_35   : eRE.MsgType                                      = parts[1];                   break;
                        //case EBase.__TAG_49   : eRE.SenderCompID                                 = parts[1];                   break;
                        //case EBase.__TAG_56   : eRE.TargetCompID                                 = parts[1];                   break;
						case EBase.__TAG_34     : eRE.MsgSeqNum                                    = Convert.ToInt64(parts[1]);  break;
                        //case EBase.__TAG_52   : eRE.SendingTime                                  = parts[1];                   break;
                        ////====================================================================================
						case EBase.__TAG_30001: eRE.MarketID                                       = parts[1];                   break;
						case EBase.__TAG_20004: eRE.BoardID                                        = parts[1];                   break;
						case EBase.__TAG_55   : eRE.Symbol                                         = parts[1];                   break;
                        //case EBase.__TAG_60   : eRE.TransactTime                                 = parts[1];                   break;
                        //case EBase.__TAG_30615: eRE.RandomEndApplyClassification                 = parts[1];                   break;
						case EBase.__TAG_30616  : eRE.RandomEndTentativeExecutionPrice = Convert.ToDouble(parts[1]); break;
                        //case EBase.__TAG_30617: eRE.RandomEndEstimatedHighestPrice               = Convert.ToDouble(parts[1]); break;
                        //case EBase.__TAG_30618: eRE.RandomEndEstimatedHighestPriceDisparateRatio = Convert.ToDouble(parts[1]); break;
                        //case EBase.__TAG_30619: eRE.RandomEndEstimatedLowestPrice                = Convert.ToDouble(parts[1]); break;
                        //case EBase.__TAG_30620: eRE.RandomEndEstimatedLowestPriceDisparateRatio  = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_30621: eRE.LatestPrice                                  = Convert.ToDouble(parts[1]); break;
                            //case EBase.__TAG_30622: eRE.LatestPriceDisparateRatio                    = Convert.ToDouble(parts[1]); break;
                            //case EBase.__TAG_30623: eRE.RandomEndReleaseTimes                        = parts[1];                   break;
                            ////====================================================================================
                            //case EBase.__TAG_10   : eRE.CheckSum                                     = parts[1];                   break;
                    }
                }
            }

            return eRE;
        }


        /// <summary>
        /// 2020-09-09 16:12:26 ngocta2
        /// chuyen raw data (fix format) sang object EForeignerOrderLimit (ME)
        /// chi giu lai cac field can thiet, ko can thi bo het (comment)
        /// </summary>
        /// <param name="rawData"></param>
        /// <returns></returns>
        public EDeemTradePrice Fix_Fix2EDeemTradePrice(string rawData, int priceDividedBy = 1, int priceRoundDigitsCount = 2, int massDividedBy = 1)
        {
            EDeemTradePrice eDTP = new EDeemTradePrice();
            string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            foreach (string pair in arr)
            {
                if (!string.IsNullOrEmpty(pair))
                {
                    string[] parts = pair.Split(__STRING_EQUAL);
                    switch (parts[0])
                    {
						case EBase.__TAG_8    : eDTP.BeginString        = parts[1];                   break;
					    case EBase.__TAG_9    : eDTP.BodyLength         = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_35   : eDTP.MsgType            = parts[1];                   break;
                        //case EBase.__TAG_49   : eDTP.SenderCompID       = parts[1];                   break;
                        //case EBase.__TAG_56   : eDTP.TargetCompID       = parts[1];                   break;
						case EBase.__TAG_34     : eDTP.MsgSeqNum          = Convert.ToInt64(parts[1]);  break;
                        case EBase.__TAG_52: eDTP.SendingTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(parts[1], "yyyyMMdd HH:mm:ss.fff", null), timeZone).ToString("yyyyMMdd HH:mm:ss.fff"); break;
                        ////====================================================================================
						case EBase.__TAG_30001  : eDTP.MarketID           = parts[1];                   break;
						case EBase.__TAG_20004  : eDTP.BoardID            = parts[1];                   break;
						case EBase.__TAG_55     : eDTP.Symbol             = parts[1];                   break;
						case EBase.__TAG_30552	: eDTP.ExpectedTradePx	  = this.ProcessPrice(parts[1], priceDividedBy, priceRoundDigitsCount); break;//Convert.ToDouble(parts[1]); break;
                        case EBase.__TAG_30553	: eDTP.ExpectedTradeQty   = this.Processkl(parts[1], massDividedBy); break;//Convert.ToInt64(parts[1]);  break;
                                                                                                                       //case EBase.__TAG_30554: eDTP.ExpectedTradeYield = Convert.ToDouble(parts[1]); break;
                                                                                                                       ////====================================================================================
                                                                                                                       //case EBase.__TAG_10   : eDTP.CheckSum           = parts[1];                   break;
                    }
                }
            }

            return eDTP;
        }


        /// <summary>
        /// 2020-09-10 16:43:09 ngocta2
        /// chuyen raw data thanh EBasePrice		
        /// </summary>
        /// <param name="rawData">8=FIX.4.49=69235=X49=VNMGW56=9999934=1335552=20190517 10:35:53.79630001=STO20004=G4336=4055=VN718148000675=2019022160=10355378830521=030522=030523=030524=0268=1083=1279=0269=1290=1270=0.0271=0346=030271=083=2279=0269=0290=1270=118500.0271=8346=030271=083=3279=0269=1290=2270=0.0271=0346=030271=083=4279=0269=0290=2270=114500.0271=152346=030271=083=5279=0269=1290=3270=0.0271=0346=030271=083=6279=0269=0290=3270=114000.0271=54346=030271=083=7279=0269=1290=4270=0.0271=0346=030271=083=8279=0269=0290=4270=0.0271=0346=030271=083=9279=0269=1290=5270=0.0271=0346=030271=083=10279=0269=0290=5270=0.0271=0346=030271=010=186</param>
        /// <param name="readAllTags">
        /// false - ko can lay tat ca data, can xu ly data nhanh trong feeder de pub ra hub
        /// true  - lay tat ca data de insert db
        /// </param>
        /// <returns>1</returns>		
        public EBasePrice Fix_Fix2EBasePrice(string rawData, bool readAllTags = false, int priceDividedBy = 1, int priceRoundDigitsCount = 2, int massDividedBy = 1)
        {
			int rptSeq            = 0;		 // 83		Data sequential number within repeated record
            string mdUpdateAction = null;    // 279		Type code of update action for an entry of market data: 0 = New
			string mdEntryType    = null;    // 269		Data classification for an entry of market data: 0 = Bid; 1 = Offer; 2 = Trade; 4 = Opening Price; 5 = Closing Price; 7 = Trading Session High Price; 8 = Trading Session Low Price
            int mdEntryPositionNo = 0;       // 290		Position no (or level) for an entry of market data
			double mdEntryPx      = 0;       // 270		Price for an entry of market data
			int mdEntrySize       = 0;       // 271		Size (or quantity) for an entry of market data
			int numberOfOrders    = 0;       // 346		Number of orders
			double mdEntryYield   = 0;       // 30270	Yield of the entry. Bond market only.
			int mdEntryMMSize     = 0;       // 30271	Size of the entry provided from market makers
			string sep1           = __STRING_FIX_SEPARATOR + EBasePrice.__TAG_83 + __STRING_EQUAL;// "83="
			string sep2           = EBasePrice.__TAG_83 + __STRING_EQUAL;         // "83="
            EBasePrice eBP = new EBasePrice(rawData, null);

            if (rawData.Contains(sep1))
            {
                string data = rawData.Substring(rawData.IndexOf(sep1));    // 83=1279=0269=1290=1270=0.0271=0346=030271=083=2279=0269=0290=1270=118500.0271=8346=030

                string[] bigArray = data.Split(sep1);    // {"1279=0269=1290=1270=0.0271=0346=030271=0", "2279=0269=0290=1270=118500.0271=8346=030271=0", ...}
                StringBuilder sb = new StringBuilder("");      //   {"data":[{"83":"1","270":"aaa"},{"83":"2","270":"bbbbb"}]}
                for (int i = 0; i < bigArray.Length; i++)
                {
                    if (!string.IsNullOrEmpty(bigArray[i]))
                    {
                        // lay value tu raw string
                        string pair = sep2 + bigArray[i]; // 83=1279=0269=1290=1270=0.0271=0346=030271=0
                        string[] smallArray = pair.Split(__STRING_FIX_SEPARATOR); // {"83=1","279=0","269=1",...}
                        for (int j = 0; j < smallArray.Length; j++)
                        {
                            string[] arr = smallArray[j].Split(__STRING_EQUAL); // {"83","1"} 
                            switch (arr[0])
                            {

                                case EBase.__TAG_83: rptSeq = Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_279: mdUpdateAction = arr[1]; break;
                                case EBase.__TAG_269: mdEntryType = arr[1]; break;
                                case EBase.__TAG_290: mdEntryPositionNo = Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_270: mdEntryPx = this.ProcessPrice(arr[1], priceDividedBy, priceRoundDigitsCount); break;
                                case EBase.__TAG_271: mdEntrySize = this.Processkl(arr[1], massDividedBy); break; //Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_346: numberOfOrders = Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_30270: mdEntryYield = Convert.ToDouble(arr[1]); break;
                                case EBase.__TAG_30271: mdEntryMMSize = this.Processkl(arr[1], massDividedBy); break;//Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_387: eBP.TotalVolumeTraded = this.Processkl(arr[1], massDividedBy); break;
                                case EBase.__TAG_381: eBP.GrossTradeAmt = Convert.ToDouble(arr[1]); break;


                                    // case EBase.__TAG_75   : eBP.TradeDate         = arr[1];                   break;
                                    // case EBase.__TAG_60   : eBP.TransactTime      = arr[1];                   break;
                            }
                        }

                        // gan value vao entity
                        int entryType = Convert.ToInt32(mdEntryType);
                        switch (entryType)
                        {
                            case (int)EBasePrice.EntryTypes.Bid: SetValues(ref eBP, EBasePrice.EntryTypes.Bid, rptSeq, mdUpdateAction, mdEntryPositionNo, mdEntryPx, mdEntrySize, numberOfOrders, mdEntryYield, mdEntryMMSize); eBP.Side = "B"; break;
                            case (int)EBasePrice.EntryTypes.Offer: SetValues(ref eBP, EBasePrice.EntryTypes.Offer, rptSeq, mdUpdateAction, mdEntryPositionNo, mdEntryPx, mdEntrySize, numberOfOrders, mdEntryYield, mdEntryMMSize); eBP.Side = "S"; break;
                            case (int)EBasePrice.EntryTypes.Trade: eBP.MatchPrice = mdEntryPx; eBP.MatchQuantity = Convert.ToInt64(mdEntrySize); break;
                            case (int)EBasePrice.EntryTypes.OpenPrice: eBP.OpenPrice = mdEntryPx; eBP.OpenPriceQty = Convert.ToInt64(mdEntrySize); break;
                            case (int)EBasePrice.EntryTypes.ClosePrice: eBP.ClosePrice = mdEntryPx; break;
                            case (int)EBasePrice.EntryTypes.HighPrice: eBP.HighestPrice = mdEntryPx; break;
                            case (int)EBasePrice.EntryTypes.LowPrice: eBP.LowestPrice = mdEntryPx; break;
                        }

                        // repeatingJson 
                        // sb.Append(this.Fix_Fix2Json(pair) + EGlobalConfig.__STRING_COMMA);
                    }
                }

                // bo ky tu cuoi cung
                //sb.Length--;
                // sb.Insert(0, "{\"data\":[");
                // sb.Append("]}");
                // done
                //eBP.RepeatingDataJson = sb.ToString();
                // eBP.RepeatingDataFix = rawData;

            }

            // lay tat ca data, chi su dung khi can insert db
            if (readAllTags)
            {
                string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
                foreach (string pair in arr)
                {
                    if (!string.IsNullOrEmpty(pair))
                    {
                        string[] parts = pair.Split(__STRING_EQUAL);
                        switch (parts[0])
                        {
							case EBase.__TAG_8      : eBP.BeginString       = parts[1]; break;
							case EBase.__TAG_9      : eBP.BodyLength        = Convert.ToInt64(parts[1]); break;
							case EBase.__TAG_35     : eBP.MsgType           = parts[1]; break;
							case EBase.__TAG_49     : eBP.SenderCompID      = parts[1]; break;
							case EBase.__TAG_56     : eBP.TargetCompID      = parts[1]; break;
							case EBase.__TAG_34     : eBP.MsgSeqNum         = Convert.ToInt64(parts[1]); break;
							case EBase.__TAG_52     : eBP.SendingTime       = TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(parts[1], "yyyyMMdd HH:mm:ss.fff", null), timeZone).ToString("yyyyMMdd HH:mm:ss.fff"); break;
                            //====================================================================================
							case EBase.__TAG_30001  : eBP.MarketID          = parts[1]; break;
							case EBase.__TAG_20004  : eBP.BoardID           = parts[1]; break;
							case EBase.__TAG_336    : eBP.TradingSessionID  = parts[1]; break;
							case EBase.__TAG_55     : eBP.Symbol            = parts[1]; break;
                            //case EBase.__TAG_75   : eBP.TradeDate         = parts[1];                   break;
                            //case EBase.__TAG_60   : eBP.TransactTime      = parts[1];                   break;
							case EBase.__TAG_387    : eBP.TotalVolumeTraded = Convert.ToInt64(parts[1]); break;
							case EBase.__TAG_381    : eBP.GrossTradeAmt     = Convert.ToDouble(parts[1]); break;
							case EBase.__TAG_30521  : eBP.SellTotOrderQty   = Convert.ToInt64(parts[1]); break;
							case EBase.__TAG_30522  : eBP.BuyTotOrderQty    = Convert.ToInt64(parts[1]); break;
							case EBase.__TAG_30523  : eBP.SellValidOrderCnt = Convert.ToInt64(parts[1]); break;
							case EBase.__TAG_30524  : eBP.BuyValidOrderCnt  = Convert.ToInt64(parts[1]); break;
							case EBase.__TAG_268    : eBP.NoMDEntries       = Convert.ToInt64(parts[1]); break;
							case EBase.__TAG_346     : eBP.NumberOfOrders    = Convert.ToInt64(parts[1]); break;
                            // Repeating Group ............... (xem code o tren)
                            //====================================================================================
                            case EBase.__TAG_10     : eBP.CheckSum          = parts[1]; break;
                        }
                    }
                }
            }


            return eBP;
        }

        /// <summary>
        /// linhnh 13/11/2023 
        /// chuyen raw data thanh EBasePrice		
        /// </summary>
        /// <param name="rawData">8=FIX.4.49=69235=X49=VNMGW56=9999934=1335552=20190517 10:35:53.79630001=STO20004=G4336=4055=VN718148000675=2019022160=10355378830521=030522=030523=030524=0268=1083=1279=0269=1290=1270=0.0271=0346=030271=083=2279=0269=0290=1270=118500.0271=8346=030271=083=3279=0269=1290=2270=0.0271=0346=030271=083=4279=0269=0290=2270=114500.0271=152346=030271=083=5279=0269=1290=3270=0.0271=0346=030271=083=6279=0269=0290=3270=114000.0271=54346=030271=083=7279=0269=1290=4270=0.0271=0346=030271=083=8279=0269=0290=4270=0.0271=0346=030271=083=9279=0269=1290=5270=0.0271=0346=030271=083=10279=0269=0290=5270=0.0271=0346=030271=010=186</param>
        /// <param name="readAllTags">
        /// false - ko can lay tat ca data, can xu ly data nhanh trong feeder de pub ra hub
        /// true  - lay tat ca data de insert db
        /// </param>
        /// <returns>1</returns>		
        public EBasePrice_hsx Fix_Fix2EBasePrice_hsx(string rawData, bool readAllTags = false, int priceDividedBy = 1000, int priceRoundDigitsCount = 2, int massDividedBy = 100)
        {
            int rptSeq = 0;      // 83		Data sequential number within repeated record
            string mdUpdateAction = null;    // 279		Type code of update action for an entry of market data: 0 = New
            string mdEntryType = null;    // 269		Data classification for an entry of market data: 0 = Bid; 1 = Offer; 2 = Trade; 4 = Opening Price; 5 = Closing Price; 7 = Trading Session High Price; 8 = Trading Session Low Price
            int mdEntryPositionNo = 0;       // 290		Position no (or level) for an entry of market data
            double mdEntryPx = 0;       // 270		Price for an entry of market data
            int mdEntrySize = 0;       // 271		Size (or quantity) for an entry of market data
            int numberOfOrders = 0;       // 346		Number of orders
            double mdEntryYield = 0;       // 30270	Yield of the entry. Bond market only.
            int mdEntryMMSize = 0;       // 30271	Size of the entry provided from market makers
            string sep1 = __STRING_FIX_SEPARATOR + EBasePrice.__TAG_83 + __STRING_EQUAL;// "83="
            string sep2 = EBasePrice_hsx.__TAG_83 + __STRING_EQUAL;         // "83="
            EBasePrice_hsx eBP = new EBasePrice_hsx(rawData, null);

            if (rawData.Contains(sep1))
            {
                string data = rawData.Substring(rawData.IndexOf(sep1));    // 83=1279=0269=1290=1270=0.0271=0346=030271=083=2279=0269=0290=1270=118500.0271=8346=030

                string[] bigArray = data.Split(sep1);    // {"1279=0269=1290=1270=0.0271=0346=030271=0", "2279=0269=0290=1270=118500.0271=8346=030271=0", ...}
                StringBuilder sb = new StringBuilder("");      //   {"data":[{"83":"1","270":"aaa"},{"83":"2","270":"bbbbb"}]}
                for (int i = 0; i < bigArray.Length; i++)
                {
                    if (!string.IsNullOrEmpty(bigArray[i]))
                    {
                        // lay value tu raw string
                        string pair = sep2 + bigArray[i]; // 83=1279=0269=1290=1270=0.0271=0346=030271=0
                        string[] smallArray = pair.Split(__STRING_FIX_SEPARATOR); // {"83=1","279=0","269=1",...}
                        for (int j = 0; j < smallArray.Length; j++)
                        {
                            string[] arr = smallArray[j].Split(__STRING_EQUAL); // {"83","1"} 
                            switch (arr[0])
                            {

                                case EBase.__TAG_83: rptSeq = Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_279: mdUpdateAction = arr[1]; break;
                                case EBase.__TAG_269: mdEntryType = arr[1]; break;
                                case EBase.__TAG_290: mdEntryPositionNo = Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_270: mdEntryPx = this.ProcessPrice(arr[1], priceDividedBy, priceRoundDigitsCount); break;
                                case EBase.__TAG_271: mdEntrySize = this.Processkl(arr[1], massDividedBy); break; //Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_346: numberOfOrders = Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_30270: mdEntryYield = Convert.ToDouble(arr[1]); break;
                                case EBase.__TAG_30271: mdEntryMMSize = this.Processkl(arr[1], massDividedBy); break;//Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_387: eBP.TotalVolumeTraded = this.Processkl(arr[1], massDividedBy); break;
                                case EBase.__TAG_381: eBP.GrossTradeAmt = Convert.ToDouble(arr[1]); break;
                            }
                        }

                        // gan value vao entity
                        int entryType = Convert.ToInt32(mdEntryType);
                        switch (entryType)
                        {
                            case (int)EBasePrice_hsx.EntryTypes.Bid: SetValues_hsx(ref eBP, EBasePrice_hsx.EntryTypes.Bid, rptSeq, mdUpdateAction, mdEntryPositionNo, mdEntryPx, mdEntrySize, numberOfOrders, mdEntryYield, mdEntryMMSize); eBP.Side = "B"; break;
                            case (int)EBasePrice_hsx.EntryTypes.Offer: SetValues_hsx(ref eBP, EBasePrice_hsx.EntryTypes.Offer, rptSeq, mdUpdateAction, mdEntryPositionNo, mdEntryPx, mdEntrySize, numberOfOrders, mdEntryYield, mdEntryMMSize); eBP.Side = "S"; break;
                            case (int)EBasePrice_hsx.EntryTypes.Trade: eBP.MatchPrice = mdEntryPx; eBP.MatchQuantity = Convert.ToInt64(mdEntrySize); break;
                            case (int)EBasePrice_hsx.EntryTypes.OpenPrice: eBP.OpenPrice = mdEntryPx; eBP.OpenPriceQty = Convert.ToInt64(mdEntrySize); break;
                            case (int)EBasePrice_hsx.EntryTypes.ClosePrice: eBP.ClosePrice = mdEntryPx; break;
                            case (int)EBasePrice_hsx.EntryTypes.HighPrice: eBP.HighestPrice = mdEntryPx; break;
                            case (int)EBasePrice_hsx.EntryTypes.LowPrice: eBP.LowestPrice = mdEntryPx; break;
                        }

                        // repeatingJson 
                        //sb.Append(this.Fix_Fix2Json(pair) + EGlobalConfig.__STRING_COMMA);
                    }
                }

                // bo ky tu cuoi cung
                //sb.Length--;
                //sb.Insert(0, "{\"data\":[");
                //sb.Append("]}");
                //// done
                //eBP.RepeatingDataJson = sb.ToString();
                //eBP.RepeatingDataFix = rawData;

            }

            // lay tat ca data, chi su dung khi can insert db
            if (readAllTags)
            {
                string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
                foreach (string pair in arr)
                {
                    if (!string.IsNullOrEmpty(pair))
                    {
                        string[] parts = pair.Split(__STRING_EQUAL);
                        switch (parts[0])
                        {
                            case EBase.__TAG_8: eBP.BeginString = parts[1]; break;
                            case EBase.__TAG_9: eBP.BodyLength = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_35: eBP.MsgType = parts[1]; break;
                            case EBase.__TAG_49: eBP.SenderCompID = parts[1]; break;
                            case EBase.__TAG_56: eBP.TargetCompID = parts[1]; break;
                            case EBase.__TAG_34: eBP.MsgSeqNum = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_52: eBP.SendingTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(parts[1], "yyyyMMdd HH:mm:ss.fff", null), timeZone).ToString("yyyyMMdd HH:mm:ss.fff"); break;
                            //====================================================================================
                            case EBase.__TAG_30001: eBP.MarketID = parts[1]; break;
                            case EBase.__TAG_20004: eBP.BoardID = parts[1]; break;
                            case EBase.__TAG_336: eBP.TradingSessionID = parts[1]; break;
                            case EBase.__TAG_55: eBP.Symbol = parts[1]; break;
                            //case EBase.__TAG_75   : eBP.TradeDate         = parts[1];                   break;
                            //case EBase.__TAG_60   : eBP.TransactTime      = parts[1];                   break;
                            case EBase.__TAG_387: eBP.TotalVolumeTraded = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_381: eBP.GrossTradeAmt = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_30521: eBP.SellTotOrderQty = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_30522: eBP.BuyTotOrderQty = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_30523: eBP.SellValidOrderCnt = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_30524: eBP.BuyValidOrderCnt = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_268: eBP.NoMDEntries = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_346: eBP.NumberOfOrders = Convert.ToInt64(parts[1]); break;
                            // Repeating Group ............... (xem code o tren)
                            //====================================================================================
                            case EBase.__TAG_10: eBP.CheckSum = parts[1]; break;
                        }
                    }
                }
            }

            return eBP;
        }

        /// <summary>
        /// 2023-03-24 Convert msg M7 thay đổi trần , sàn , tc của mã chứng khoán trong giờ
        /// chuyen raw data (fix format) sang object ESecurityDefinition (d)
        /// chi giu lai cac field can thiet, ko can thi bo het (comment)
        /// </summary>
        /// <param name="rawData">"8=FIX.4.49=52035=d49=VNMGW56=9999934=152=20190517 09:13:04.84430001=BDO20004=G1911=105207=HO55=VN0ANC11601630624=ANC1160130628=2330629=CTY DINH DUONG NN QUOC TE30630=CTY DINH DUONG NN QUOC TE20009=B1BDOBS20003=BDO30604=BS201=1194=541=106=ID00000130225=20171207231=1.0223=3.015=VND20020=30000001149=119103.01148=103521.0202=0.0965=N30631=1.01193=236=0.020013=111312.020014=0.020015=0.020016=0.0140=111312.020027=330642=2018120530511=30301=2016121930614=120018=30625=0.030635=30636=30637=10=239"</param>
        /// <returns></returns>
        public ESecurityInformationNotification Fix_Fix2SecurityInformationNoti(string rawData, int priceDividedBy = 10, int priceRoundDigitsCount = 2)
        {
            ESecurityInformationNotification eSD = new ESecurityInformationNotification();
            string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            foreach (string pair in arr)
            {
                if (!string.IsNullOrEmpty(pair))
                {
                    string[] parts = pair.Split(__STRING_EQUAL);
                    switch (parts[0])
                    {
                        case EBase.__TAG_8: eSD.BeginString = parts[1]; break;
                        case EBase.__TAG_9: eSD.BodyLength = Convert.ToInt64(parts[1]); break;
                        case EBase.__TAG_35: eSD.MsgType = parts[1]; break;
                        case EBase.__TAG_49: eSD.SenderCompID = parts[1]; break;
                        case EBase.__TAG_56: eSD.TargetCompID = parts[1]; break;
                        case EBase.__TAG_34: eSD.MsgSeqNum = Convert.ToInt64(parts[1]); break;
                        case EBase.__TAG_52: eSD.SendingTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(parts[1], "yyyyMMdd HH:mm:ss.fff", null), timeZone).ToString("yyyyMMdd HH:mm:ss.fff"); break;
                        ////===============================================================================================
                        case EBase.__TAG_30001: eSD.MarketID = parts[1]; break;
                        case EBase.__TAG_20004: eSD.BoardID = parts[1]; break;
                        case EBase.__TAG_55: eSD.Symbol = parts[1]; break;
                        case EBase.__TAG_20020: eSD.ListedShares = Convert.ToInt64(parts[1]); break;
                        case EBase.__TAG_1149: eSD.HighLimitPrice = this.ProcessPrice(parts[1], priceDividedBy, priceRoundDigitsCount); break;
                        case EBase.__TAG_1148: eSD.LowLimitPrice = this.ProcessPrice(parts[1], priceDividedBy, priceRoundDigitsCount); break;
                        case EBase.__TAG_20013: eSD.ReferencePrice = this.ProcessPrice(parts[1], priceDividedBy, priceRoundDigitsCount); break;
                        case EBase.__TAG_20014: eSD.EvaluationPrice = Convert.ToDouble(parts[1]); break;
                        case EBase.__TAG_20018: eSD.ExClassType = parts[1]; break;
                    }
                }
            }
            return eSD;
        }

        /// <summary>
        /// 2020-09-15 16:24:42 ngocta2
        /// set value vao entity
        /// </summary>
        /// <param name="ePrice"></param>
        /// <param name="entryType"></param>
        /// <param name="rptSeq"></param>
        /// <param name="mdUpdateAction"></param>
        /// <param name="mdEntryPositionNo"></param>
        /// <param name="mdEntryPx"></param>
        /// <param name="mdEntrySize"></param>
        /// <param name="numberOfOrders"></param>
        /// <param name="mdEntryYield"></param>
        /// <param name="mdEntryMMSize"></param>
        public void SetValues(ref EBasePrice ePrice, EBasePrice.EntryTypes entryType, int rptSeq, string mdUpdateAction, int mdEntryPositionNo, double mdEntryPx, int mdEntrySize, int numberOfOrders, double mdEntryYield, int mdEntryMMSize)
        {
            switch (entryType)
            {
                case EBasePrice.EntryTypes.Bid:
                    switch (mdEntryPositionNo)
                    {
                        case 1: ePrice.BuyPrice1 = mdEntryPx; ePrice.BuyQuantity1 = mdEntrySize; ePrice.BuyPrice1_NOO = numberOfOrders; ePrice.BuyPrice1_MDEY = mdEntryYield; ePrice.BuyPrice1_MDEMMS = mdEntryMMSize; break;
                        case 2: ePrice.BuyPrice2 = mdEntryPx; ePrice.BuyQuantity2 = mdEntrySize; ePrice.BuyPrice2_NOO = numberOfOrders; ePrice.BuyPrice2_MDEY = mdEntryYield; ePrice.BuyPrice2_MDEMMS = mdEntryMMSize; break;
                        case 3: ePrice.BuyPrice3 = mdEntryPx; ePrice.BuyQuantity3 = mdEntrySize; ePrice.BuyPrice3_NOO = numberOfOrders; ePrice.BuyPrice3_MDEY = mdEntryYield; ePrice.BuyPrice3_MDEMMS = mdEntryMMSize; break;
                        case 4: ePrice.BuyPrice4 = mdEntryPx; ePrice.BuyQuantity4 = mdEntrySize; ePrice.BuyPrice4_NOO = numberOfOrders; ePrice.BuyPrice4_MDEY = mdEntryYield; ePrice.BuyPrice4_MDEMMS = mdEntryMMSize; break;
                        case 5: ePrice.BuyPrice5 = mdEntryPx; ePrice.BuyQuantity5 = mdEntrySize; ePrice.BuyPrice5_NOO = numberOfOrders; ePrice.BuyPrice5_MDEY = mdEntryYield; ePrice.BuyPrice5_MDEMMS = mdEntryMMSize; break;
                        case 6: ePrice.BuyPrice6 = mdEntryPx; ePrice.BuyQuantity6 = mdEntrySize; ePrice.BuyPrice6_NOO = numberOfOrders; ePrice.BuyPrice6_MDEY = mdEntryYield; ePrice.BuyPrice6_MDEMMS = mdEntryMMSize; break;
                        case 7: ePrice.BuyPrice7 = mdEntryPx; ePrice.BuyQuantity7 = mdEntrySize; ePrice.BuyPrice7_NOO = numberOfOrders; ePrice.BuyPrice7_MDEY = mdEntryYield; ePrice.BuyPrice7_MDEMMS = mdEntryMMSize; break;
                        case 8: ePrice.BuyPrice8 = mdEntryPx; ePrice.BuyQuantity8 = mdEntrySize; ePrice.BuyPrice8_NOO = numberOfOrders; ePrice.BuyPrice8_MDEY = mdEntryYield; ePrice.BuyPrice8_MDEMMS = mdEntryMMSize; break;
                        case 9: ePrice.BuyPrice9 = mdEntryPx; ePrice.BuyQuantity9 = mdEntrySize; ePrice.BuyPrice9_NOO = numberOfOrders; ePrice.BuyPrice9_MDEY = mdEntryYield; ePrice.BuyPrice9_MDEMMS = mdEntryMMSize; break;
                        case 10: ePrice.BuyPrice10 = mdEntryPx; ePrice.BuyQuantity10 = mdEntrySize; ePrice.BuyPrice10_NOO = numberOfOrders; ePrice.BuyPrice10_MDEY = mdEntryYield; ePrice.BuyPrice10_MDEMMS = mdEntryMMSize; break;
                    }//switch (mdEntryPositionNo)
                    break;
                case EBasePrice.EntryTypes.Offer:
                    switch (mdEntryPositionNo)
                    {
                        case 1: ePrice.SellPrice1 = mdEntryPx; ePrice.SellQuantity1 = mdEntrySize; ePrice.SellPrice1_NOO = numberOfOrders; ePrice.SellPrice1_MDEY = mdEntryYield; ePrice.SellPrice1_MDEMMS = mdEntryMMSize; break;
                        case 2: ePrice.SellPrice2 = mdEntryPx; ePrice.SellQuantity2 = mdEntrySize; ePrice.SellPrice2_NOO = numberOfOrders; ePrice.SellPrice2_MDEY = mdEntryYield; ePrice.SellPrice2_MDEMMS = mdEntryMMSize; break;
                        case 3: ePrice.SellPrice3 = mdEntryPx; ePrice.SellQuantity3 = mdEntrySize; ePrice.SellPrice3_NOO = numberOfOrders; ePrice.SellPrice3_MDEY = mdEntryYield; ePrice.SellPrice3_MDEMMS = mdEntryMMSize; break;
                        case 4: ePrice.SellPrice4 = mdEntryPx; ePrice.SellQuantity4 = mdEntrySize; ePrice.SellPrice4_NOO = numberOfOrders; ePrice.SellPrice4_MDEY = mdEntryYield; ePrice.SellPrice4_MDEMMS = mdEntryMMSize; break;
                        case 5: ePrice.SellPrice5 = mdEntryPx; ePrice.SellQuantity5 = mdEntrySize; ePrice.SellPrice5_NOO = numberOfOrders; ePrice.SellPrice5_MDEY = mdEntryYield; ePrice.SellPrice5_MDEMMS = mdEntryMMSize; break;
                        case 6: ePrice.SellPrice6 = mdEntryPx; ePrice.SellQuantity6 = mdEntrySize; ePrice.SellPrice6_NOO = numberOfOrders; ePrice.SellPrice6_MDEY = mdEntryYield; ePrice.SellPrice6_MDEMMS = mdEntryMMSize; break;
                        case 7: ePrice.SellPrice7 = mdEntryPx; ePrice.SellQuantity7 = mdEntrySize; ePrice.SellPrice7_NOO = numberOfOrders; ePrice.SellPrice7_MDEY = mdEntryYield; ePrice.SellPrice7_MDEMMS = mdEntryMMSize; break;
                        case 8: ePrice.SellPrice8 = mdEntryPx; ePrice.SellQuantity8 = mdEntrySize; ePrice.SellPrice8_NOO = numberOfOrders; ePrice.SellPrice8_MDEY = mdEntryYield; ePrice.SellPrice8_MDEMMS = mdEntryMMSize; break;
                        case 9: ePrice.SellPrice9 = mdEntryPx; ePrice.SellQuantity9 = mdEntrySize; ePrice.SellPrice9_NOO = numberOfOrders; ePrice.SellPrice9_MDEY = mdEntryYield; ePrice.SellPrice9_MDEMMS = mdEntryMMSize; break;
                        case 10: ePrice.SellPrice10 = mdEntryPx; ePrice.SellQuantity10 = mdEntrySize; ePrice.SellPrice10_NOO = numberOfOrders; ePrice.SellPrice10_MDEY = mdEntryYield; ePrice.SellPrice10_MDEMMS = mdEntryMMSize; break;
                    }//switch (mdEntryPositionNo)
                    break;
            } // switch (entryType)

        }

        /// <summary>
        /// Linhnh 13/11/2023
        /// </summary>
        /// <param name="ePrice"></param>
        /// <param name="entryType"></param>
        /// <param name="rptSeq"></param>
        /// <param name="mdUpdateAction"></param>
        /// <param name="mdEntryPositionNo"></param>
        /// <param name="mdEntryPx"></param>
        /// <param name="mdEntrySize"></param>
        /// <param name="numberOfOrders"></param>
        /// <param name="mdEntryYield"></param>
        /// <param name="mdEntryMMSize"></param>
        public void SetValues_hsx(ref EBasePrice_hsx ePrice, EBasePrice_hsx.EntryTypes entryType, int rptSeq, string mdUpdateAction, int mdEntryPositionNo, double mdEntryPx, int mdEntrySize, int numberOfOrders, double mdEntryYield, int mdEntryMMSize)
        {
            switch (entryType)
            {
                case EBasePrice_hsx.EntryTypes.Bid:
                    switch (mdEntryPositionNo)
                    {
                        case 1: ePrice.BuyPrice1 = mdEntryPx; ePrice.BuyQuantity1 = mdEntrySize; ePrice.BuyPrice1_NOO = numberOfOrders; ePrice.BuyPrice1_MDEY = mdEntryYield; ePrice.BuyPrice1_MDEMMS = mdEntryMMSize; break;
                        case 2: ePrice.BuyPrice2 = mdEntryPx; ePrice.BuyQuantity2 = mdEntrySize; ePrice.BuyPrice2_NOO = numberOfOrders; ePrice.BuyPrice2_MDEY = mdEntryYield; ePrice.BuyPrice2_MDEMMS = mdEntryMMSize; break;
                        case 3: ePrice.BuyPrice3 = mdEntryPx; ePrice.BuyQuantity3 = mdEntrySize; ePrice.BuyPrice3_NOO = numberOfOrders; ePrice.BuyPrice3_MDEY = mdEntryYield; ePrice.BuyPrice3_MDEMMS = mdEntryMMSize; break;
                        case 4: ePrice.BuyPrice4 = mdEntryPx; ePrice.BuyQuantity4 = mdEntrySize; ePrice.BuyPrice4_NOO = numberOfOrders; ePrice.BuyPrice4_MDEY = mdEntryYield; ePrice.BuyPrice4_MDEMMS = mdEntryMMSize; break;
                        case 5: ePrice.BuyPrice5 = mdEntryPx; ePrice.BuyQuantity5 = mdEntrySize; ePrice.BuyPrice5_NOO = numberOfOrders; ePrice.BuyPrice5_MDEY = mdEntryYield; ePrice.BuyPrice5_MDEMMS = mdEntryMMSize; break;
                        case 6: ePrice.BuyPrice6 = mdEntryPx; ePrice.BuyQuantity6 = mdEntrySize; ePrice.BuyPrice6_NOO = numberOfOrders; ePrice.BuyPrice6_MDEY = mdEntryYield; ePrice.BuyPrice6_MDEMMS = mdEntryMMSize; break;
                        case 7: ePrice.BuyPrice7 = mdEntryPx; ePrice.BuyQuantity7 = mdEntrySize; ePrice.BuyPrice7_NOO = numberOfOrders; ePrice.BuyPrice7_MDEY = mdEntryYield; ePrice.BuyPrice7_MDEMMS = mdEntryMMSize; break;
                        case 8: ePrice.BuyPrice8 = mdEntryPx; ePrice.BuyQuantity8 = mdEntrySize; ePrice.BuyPrice8_NOO = numberOfOrders; ePrice.BuyPrice8_MDEY = mdEntryYield; ePrice.BuyPrice8_MDEMMS = mdEntryMMSize; break;
                        case 9: ePrice.BuyPrice9 = mdEntryPx; ePrice.BuyQuantity9 = mdEntrySize; ePrice.BuyPrice9_NOO = numberOfOrders; ePrice.BuyPrice9_MDEY = mdEntryYield; ePrice.BuyPrice9_MDEMMS = mdEntryMMSize; break;
                        case 10: ePrice.BuyPrice10 = mdEntryPx; ePrice.BuyQuantity10 = mdEntrySize; ePrice.BuyPrice10_NOO = numberOfOrders; ePrice.BuyPrice10_MDEY = mdEntryYield; ePrice.BuyPrice10_MDEMMS = mdEntryMMSize; break;
                    }//switch (mdEntryPositionNo)
                    break;
                case EBasePrice_hsx.EntryTypes.Offer:
                    switch (mdEntryPositionNo)
                    {
                        case 1: ePrice.SellPrice1 = mdEntryPx; ePrice.SellQuantity1 = mdEntrySize; ePrice.SellPrice1_NOO = numberOfOrders; ePrice.SellPrice1_MDEY = mdEntryYield; ePrice.SellPrice1_MDEMMS = mdEntryMMSize; break;
                        case 2: ePrice.SellPrice2 = mdEntryPx; ePrice.SellQuantity2 = mdEntrySize; ePrice.SellPrice2_NOO = numberOfOrders; ePrice.SellPrice2_MDEY = mdEntryYield; ePrice.SellPrice2_MDEMMS = mdEntryMMSize; break;
                        case 3: ePrice.SellPrice3 = mdEntryPx; ePrice.SellQuantity3 = mdEntrySize; ePrice.SellPrice3_NOO = numberOfOrders; ePrice.SellPrice3_MDEY = mdEntryYield; ePrice.SellPrice3_MDEMMS = mdEntryMMSize; break;
                        case 4: ePrice.SellPrice4 = mdEntryPx; ePrice.SellQuantity4 = mdEntrySize; ePrice.SellPrice4_NOO = numberOfOrders; ePrice.SellPrice4_MDEY = mdEntryYield; ePrice.SellPrice4_MDEMMS = mdEntryMMSize; break;
                        case 5: ePrice.SellPrice5 = mdEntryPx; ePrice.SellQuantity5 = mdEntrySize; ePrice.SellPrice5_NOO = numberOfOrders; ePrice.SellPrice5_MDEY = mdEntryYield; ePrice.SellPrice5_MDEMMS = mdEntryMMSize; break;
                        case 6: ePrice.SellPrice6 = mdEntryPx; ePrice.SellQuantity6 = mdEntrySize; ePrice.SellPrice6_NOO = numberOfOrders; ePrice.SellPrice6_MDEY = mdEntryYield; ePrice.SellPrice6_MDEMMS = mdEntryMMSize; break;
                        case 7: ePrice.SellPrice7 = mdEntryPx; ePrice.SellQuantity7 = mdEntrySize; ePrice.SellPrice7_NOO = numberOfOrders; ePrice.SellPrice7_MDEY = mdEntryYield; ePrice.SellPrice7_MDEMMS = mdEntryMMSize; break;
                        case 8: ePrice.SellPrice8 = mdEntryPx; ePrice.SellQuantity8 = mdEntrySize; ePrice.SellPrice8_NOO = numberOfOrders; ePrice.SellPrice8_MDEY = mdEntryYield; ePrice.SellPrice8_MDEMMS = mdEntryMMSize; break;
                        case 9: ePrice.SellPrice9 = mdEntryPx; ePrice.SellQuantity9 = mdEntrySize; ePrice.SellPrice9_NOO = numberOfOrders; ePrice.SellPrice9_MDEY = mdEntryYield; ePrice.SellPrice9_MDEMMS = mdEntryMMSize; break;
                        case 10: ePrice.SellPrice10 = mdEntryPx; ePrice.SellQuantity10 = mdEntrySize; ePrice.SellPrice10_NOO = numberOfOrders; ePrice.SellPrice10_MDEY = mdEntryYield; ePrice.SellPrice10_MDEMMS = mdEntryMMSize; break;
                    }//switch (mdEntryPositionNo)
                    break;
            } // switch (entryType)

        }

        /// <summary>
        /// 2020-07-22 15:14:42 ngocta2
        /// convert FIX string sang JSON string (muon code ben CCommon)
        /// yeu cau speed phai nhanh nhat co the
        /// input   : FIX string
        /// output  : JSON string
        /// </summary>
        /// <param name="fixString">8=FIX.4.49=51935=d49=VNMGW56=9999934=152=20190517 09:14:26.12830001=STO20004=G4911=3851207=HO55=VN000000KMR230624=KMR30628=172930629=MIRAE Joint Stock Company30630=MIRAE Joint Stock Company20009=S1STOST20003=STO30604=ST201=1194=541=106=ID00000083225=231=1.0223=0.015=VND20020=13660000001149=999999999.01148=-999999999.0202=0.0965=N30631=1.01193=236=0.020013=4930.020014=0.020015=0.020016=0.0140=4930.020027=330642=30511=30301=2017010130614=220018=0030625=0.030635=NRM30636=SNE30637=NRM10=056</param>
        /// <returns>{"8":"FIX.4.4","9":"519","35":"d","49":"VNMGW","56":"99999","34":"1","52":"20190517 09:14:26.128","30001":"STO","20004":"G4","911":"3851","207":"HO","55":"VN000000KMR2","30624":"KMR","30628":"1729","30629":"MIRAE Joint Stock Company","30630":"MIRAE Joint Stock Company","20009":"S1STOST","20003":"STO","30604":"ST","201":"","1194":"","541":"","106":"ID00000083","225":"","231":"1.0","223":"0.0","15":"VND","20020":"1366000000","1149":"999999999.0","1148":"-999999999.0","202":"0.0","965":"N","30631":"1.0","1193":"","236":"0.0","20013":"4930.0","20014":"0.0","20015":"0.0","20016":"0.0","140":"4930.0","20027":"3","30642":"","30511":"","30301":"20170101","30614":"2","20018":"00","30625":"0.0","30635":"NRM","30636":"SNE","30637":"NRM","10":"056"}</returns>
        public string Fix_Fix2Json(string fixString)
        {
            StringBuilder sb = new StringBuilder(fixString);
            if (fixString.Substring(fixString.Length - 1) == __STRING_FIX_SEPARATOR) // ky tu cuoi cung la __STRING_FIX_SEPARATOR thi xoa __STRING_FIX_SEPARATOR
                sb.Length--;
            sb.Replace(__STRING_FIX_SEPARATOR, "\",\"");
            sb.Replace(__STRING_EQUAL, "\":\"");
            sb.Append("\"}");
            sb.Insert(0, "{\"");
            return sb.ToString();
        }

        /// <summary>
        /// 2020-09-23 16:25:04 ngocta2
        /// chuyen tu raw string thanh object EPrice
        /// bao gom cac data can thiet de insert tPrice va tPriceIntraday
        /// </summary>
        /// <param name="rawData"></param>
        /// <param name="readAllTags"></param>
        /// <returns>1000</returns>
        public EPrice Fix_Fix2EPrice(string rawData, bool readAllTags = false, int priceDividedBy = 1, int priceRoundDigitsCount = 2, int massDividedBy = 1)
        {
            EBasePrice eBP = this.Fix_Fix2EBasePrice(rawData, readAllTags, priceDividedBy, priceRoundDigitsCount, massDividedBy);
            EPrice eP = new EPrice(rawData, eBP);

            // lay tat ca data, chi su dung khi can insert db
            if (readAllTags)
            {
                string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
                foreach (string pair in arr)
                {
                    if (!string.IsNullOrEmpty(pair))
                    {
                        string[] parts = pair.Split(__STRING_EQUAL);
                        switch (parts[0])
                        {

                            case EBase.__TAG_75: eP.TradeDate    = parts[1]; break;
                            case EBase.__TAG_60: eP.TransactTime = parts[1]; break;

                        }
                    }
                }
            }

            return eP;
        }

        /// <summary>
        /// LinhNH 13/11/2023 
        /// Thêm hàm cho convert msg HSX
        /// </summary>
        /// <param name="rawData"></param>
        /// <param name="readAllTags"></param>
        /// <param name="priceDividedBy"></param>
        /// <param name="priceRoundDigitsCount"></param>
        /// <param name="massDividedBy"></param>
        /// <returns></returns>
        public EPrice_hsx Fix_Fix2EPrice_hsx(string rawData, bool readAllTags = false, int priceDividedBy = 1000, int priceRoundDigitsCount = 2, int massDividedBy = 100)
        {
            EBasePrice_hsx eBP = this.Fix_Fix2EBasePrice_hsx(rawData, readAllTags, priceDividedBy, priceRoundDigitsCount, massDividedBy);
            EPrice_hsx eP = new EPrice_hsx(rawData, eBP);

            // lay tat ca data, chi su dung khi can insert db
            //if (readAllTags)
            //{
            //    string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            //    foreach (string pair in arr)
            //    {
            //        if (!string.IsNullOrEmpty(pair))
            //        {
            //            string[] parts = pair.Split(__STRING_EQUAL);
            //            switch (parts[0])
            //            {
            //                case EBase.__TAG_34: eP.MsgSeqNum = Convert.ToInt64(parts[1]); break;
            //                case EBase.__TAG_9: eP.BodyLength = Convert.ToInt64(parts[1]); break;
            //                case EBase.__TAG_30001: eP.MarketID = parts[1]; break;
            //                case EBase.__TAG_75: eP.TradeDate = parts[1]; break;
            //                case EBase.__TAG_60: eP.TransactTime = parts[1]; break;

            //            }
            //        }
            //    }
            //}

            return eP;
        }

        /// <summary>
        /// 2020-09-23 16:25:04 ngocta2
        /// chuyen tu raw string thanh object EPriceRecovery
        /// bao gom cac data can thiet de insert tPriceRecovery va tPriceRecoveryIntraday
        /// </summary>
        /// <param name="rawData"></param>
        /// <param name="readAllTags"></param>
        /// <returns></returns>
        public EPriceRecovery Fix_Fix2EPriceRecovery(string rawData, bool readAllTags = false, int priceDividedBy = 1, int priceRoundDigitsCount = 2, int massDividedBy = 1)
        {
            EBasePrice eBP = this.Fix_Fix2EBasePrice(rawData, readAllTags, priceDividedBy, priceRoundDigitsCount, massDividedBy);
            EPriceRecovery ePR = new EPriceRecovery(rawData, eBP);

            // lay tat ca data, chi su dung khi can insert db
            if (readAllTags)
            {
                string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
                foreach (string pair in arr)
                {
                    if (!string.IsNullOrEmpty(pair))
                    {
                        string[] parts = pair.Split(__STRING_EQUAL);
                        switch (parts[0])
                        {
							case EBase.__TAG_30561: ePR.OpnPx             = Convert.ToDouble(parts[1]); break;
							case EBase.__TAG_30562: ePR.TrdSessnHighPx    = Convert.ToDouble(parts[1]); break;
							case EBase.__TAG_30563: ePR.TrdSessnLowPx     = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_20026: ePR.SymbolCloseInfoPx = Convert.ToDouble(parts[1]); break;
							case EBase.__TAG_30565: ePR.OpnPxYld          = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_30566: ePR.TrdSessnHighPxYld = Convert.ToDouble(parts[1]); break;
							case EBase.__TAG_30567: ePR.TrdSessnLowPxYld  = Convert.ToDouble(parts[1]); break;
							case EBase.__TAG_30568: ePR.ClsPxYld          = Convert.ToDouble(parts[1]); break;

                        }
                    }
                }
            }

            return ePR;
        }


        public ESecurityInformationNotification Fix_Fix2ESecurityInformationNotification(string rawData, bool readAllTags = false)
        {

            ESecurityInformationNotification ePR = new ESecurityInformationNotification(rawData);
            // EBasePrice eBP = this.Fix_Fix2EBasePrice(rawData, readAllTags);
            // lay tat ca data, chi su dung khi can insert db
            //   if (readAllTags)
            {
                string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
                foreach (string pair in arr)
                {
                    if (!string.IsNullOrEmpty(pair))
                    {
                        string[] parts = pair.Split(__STRING_EQUAL);
                        switch (parts[0])
                        {
                            case EBase.__TAG_52: ePR.SendingTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(parts[1], "yyyyMMdd HH:mm:ss.fff", null), timeZone).ToString("yyyyMMdd HH:mm:ss.fff"); break;
                            case EBase.__TAG_55: ePR.Symbol = parts[1]; break;
                            case EBase.__TAG_30001: ePR.MarketID = parts[1]; break;
                            case EBase.__TAG_20013: ePR.ReferencePrice = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_1149: ePR.HighLimitPrice = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_1148: ePR.LowLimitPrice = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_20018: ePR.ExClassType = parts[1]; break;
                            case EBase.__TAG_20020: ePR.ListedShares = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_34: ePR.MsgSeqNum = Convert.ToInt64(parts[1]); break;
                        }
                    }
                }
            }

            return ePR;
        }


        public EPriceLimitExpansion Fix_Fix2EPriceLimitExpansion(string rawData, bool readAllTags = false)
        {

            EPriceLimitExpansion ePR = new EPriceLimitExpansion(rawData);

            // lay tat ca data, chi su dung khi can insert db
            //   if (readAllTags)
            {
                string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
                foreach (string pair in arr)
                {
                    if (!string.IsNullOrEmpty(pair))
                    {
                        string[] parts = pair.Split(__STRING_EQUAL);
                        switch (parts[0])
                        {
                            case EBase.__TAG_55: ePR.Symbol = parts[1]; break;
                            case EBase.__TAG_1149: ePR.HighLimitPrice = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_1148: ePR.LowLimitPrice = Convert.ToDouble(parts[1]); break;


                        }
                    }
                }
            }

            return ePR;
        }
        public EOpenInterest Fix_Fix2EOpenInterest(string rawData)
        {
            EOpenInterest Op = new EOpenInterest(rawData);

            // lay tat ca data, chi su dung khi can insert db
            string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            foreach (string pair in arr)
            {
                if (!string.IsNullOrEmpty(pair))
                {
                    string[] parts = pair.Split(__STRING_EQUAL);
                    switch (parts[0])
                    {
                        case EBase.__TAG_30540: Op.OpenInterestQty = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_55:    Op.Symbol          = parts[1]; break;

                    }
                }
            }


            return Op;
        }

        /// <summary>
        /// 2020-10-02 13:45:52 ngocta2
        /// chuyen object (entity) thanh json string
        /// 2021-04-15 13:31:37 ngocta2 bo AllowPrivateExcludeNullSnakeCase
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public string Utf8Json_SerializeObject(object obj)
        {
            JsonSerializer.SetDefaultResolver(StandardResolver.AllowPrivateExcludeNullCamelCase); // bo qua cac field bi null. vd: int? 
            byte[] buffer = Utf8Json.JsonSerializer.Serialize(obj);
            string json = Encoding.UTF8.GetString(buffer, 0, buffer.Length);
            return json;
        }

        /// <summary>
        /// giong het Utf8Json_SerializeObject
        /// boc them tag Data va Time
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public string Utf8Json_SerializeObjectWrap(object obj)
        {
            EDataSingle wrappedData = new EDataSingle(obj);
            return Utf8Json_SerializeObject(wrappedData);
        }

        /// <summary>
        /// chu y: tat ca field trong obj can deserialize phai co attribute DataMember >> thieu la ra null
        /// [DataMember(Name = "Data")]
        /// [DataMember(Name = "s")]
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="json"></param>
        /// <returns></returns>
        public T Utf8Json_DeserializeObject<T>(string json)
        {
            JsonSerializer.SetDefaultResolver(StandardResolver.AllowPrivateExcludeNullCamelCase); // bo qua cac field bi null. vd: int? 			
            byte[] buffer = Encoding.UTF8.GetBytes(json);
            T obj = JsonSerializer.Deserialize<T>(buffer);
            return obj;
        }

        /// <summary>
        /// 2020-10-05 09:39:41 ngocta2
        /// so sanh 2 string = nhau theo cach nhanh nhat => nhung qua 1 lop class trung gian lai bi slow di => ko xai cach nay (xem PTString.Tests)
        /// CHOT: su dung phuong phat so sanh truyen thong >> don gian de hieu, toc do nhanh
        /// </summary>
        /// <param name="s1"></param>
        /// <param name="s2"></param>
        /// <returns></returns>
        public bool String_Compare(string s1, string s2)
        {
            return string.CompareOrdinal(s1, s2) == 0;
        }




        /// <summary>
        /// 2020-10-05 14:50:04 ngocta2
        /// chuyen raw string (FIX) thanh entity class voi toc do nhanh nhat
        /// </summary>
        /// <param name="rawData"></param>
        /// <returns></returns>
        public EIndex Fix_Fix2EIndex(string rawData)
        {
            EIndex eI = new EIndex(rawData);
            string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            foreach (string pair in arr)
            {
                if (!string.IsNullOrEmpty(pair))
                {
                    string[] parts = pair.Split(__STRING_EQUAL);
                    switch (parts[0])
                    {
                        case EBase.__TAG_9: eI.BodyLength = Convert.ToInt64(parts[1]); break;
                        case EBase.__TAG_52: eI.SendingTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(parts[1], "yyyyMMdd HH:mm:ss.fff", null), timeZone).ToString("yyyyMMdd HH:mm:ss.fff"); break;
                        case EBase.__TAG_30001: eI.MarketID                        = parts[1];                   break;
						case EBase.__TAG_336  : eI.TradingSessionID                = parts[1];                   break;
						case EBase.__TAG_60   : eI.TransactTime                    = parts[1];                   break;
						case EBase.__TAG_30167: eI.IndexsTypeCode                  = parts[1];                   break;
						case EBase.__TAG_30217: eI.ValueIndexes                    = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_387  : eI.TotalVolumeTraded               = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_381  : eI.GrossTradeAmt                   = Convert.ToDouble(parts[1]); break;
                        case EBase.__TAG_30638: eI.ContauctAccTrdvol               = Convert.ToInt64(parts[1]); break;
                        case EBase.__TAG_30639: eI.ContauctAccTrdval               = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_30640: eI.BlktrdAccTrdvol                 = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_30641: eI.BlktrdAccTrdval                 = Convert.ToDouble(parts[1]); break;
						case EBase.__TAG_30589: eI.FluctuationUpperLimitIssueCount = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_30590: eI.FluctuationUpIssueCount         = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_30591: eI.FluctuationSteadinessIssueCount = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_30592: eI.FluctuationDownIssueCount       = Convert.ToInt64(parts[1]);  break;
						case EBase.__TAG_30593: eI.FluctuationLowerLimitIssueCount = Convert.ToInt64(parts[1]);  break;
                        case EBase.__TAG_30594: eI.FluctuationUpIssueVolume        = Convert.ToInt64(parts[1]); break;
                        case EBase.__TAG_30595: eI.FluctuationDownIssueVolume      = Convert.ToInt64(parts[1]); break;
                        case EBase.__TAG_30596: eI.FluctuationSteadinessIssueVolume= Convert.ToInt64(parts[1]); break;
                    }
                }
            }

            return eI;
        }


        /// <summary>
        /// 2020-10-05 14:50:04 ngocta2
        /// chuyen raw string (FIX) thanh entity class voi toc do nhanh nhat
        /// </summary>
        /// <param name="rawData"></param>
        /// <returns></returns>
        public ESecurityStatus Fix_Fix2ESecurityStatus(string rawData)
        {
            ESecurityStatus eSS = new ESecurityStatus();
            string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            foreach (string pair in arr)
            {
                if (!string.IsNullOrEmpty(pair))
                {
                    string[] parts = pair.Split(__STRING_EQUAL);
                    switch (parts[0])
                    {
                        case EBase.__TAG_34    : eSS.MsgSeqNum = Convert.ToInt64(parts[1]); break;
                        case EBase.__TAG_30001 : eSS.MarketID         = parts[1]; break;
						case EBase.__TAG_20004 : eSS.BoardID          = parts[1]; break;
						case EBase.__TAG_20005 : eSS.BoardEvtID       = parts[1]; break;
						case EBase.__TAG_20008 : eSS.SessOpenCloseCode= parts[1]; break;
						case EBase.__TAG_55	   : eSS.Symbol		      = parts[1]; break;
                        case EBase.__TAG_52: eSS.SendingTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(parts[1], "yyyyMMdd HH:mm:ss.fff", null), timeZone).ToString("yyyyMMdd HH:mm:ss.fff"); break;
                        case EBase.__TAG_336   : eSS.TradingSessionID = parts[1]; break;						
                    }
                }
            }

            return eSS;
        }
        //CHUYỂN mess MV
        public ETimeStampPolling Fix_Fix2ETimeStampPolling(string rawData)
        {
            ETimeStampPolling MV = new ETimeStampPolling();
            string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            foreach (string pair in arr)
            {
                if (!string.IsNullOrEmpty(pair))
                {
                    string[] parts = pair.Split(__STRING_EQUAL);
                    switch (parts[0])
                    {
                        case EBase.__TAG_60: MV.TransactTime = parts[1]; break;

                    }
                }
            }
            return MV;
        }
        /// <summary>
        /// 2020-10-05 14:50:04 ngocta2
        /// chuyen raw string (FIX) thanh entity class voi toc do nhanh nhat
        /// </summary>
        /// <param name="rawData"></param>
        /// <returns></returns>
        public EIndexConstituentsInformation Fix_Fix2EIndexConstituentsInformation(string rawData)
        {
            EIndexConstituentsInformation eICI = new EIndexConstituentsInformation();
            string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            foreach (string pair in arr)
            {
                if (!string.IsNullOrEmpty(pair))
                {
                    string[] parts = pair.Split(__STRING_EQUAL);
                    switch (parts[0])
                    {
                        case EBase.__TAG_9: eICI.BodyLength = Convert.ToInt64(parts[1]); break;
                        case EBase.__TAG_52:    eICI.SendingTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(parts[1], "yyyyMMdd HH:mm:ss.fff", null), timeZone).ToString("yyyyMMdd HH:mm:ss.fff"); break;
                        case EBase.__TAG_34:    eICI.MsgSeqNum   = Convert.ToInt64(parts[1]); break;
                        case EBase.__TAG_30001: eICI.MarketID       = parts[1]; break; // STO
                        case EBase.__TAG_30167: eICI.IndexsTypeCode = parts[1]; break; // VN30
						case EBase.__TAG_30606: eICI.TotalMsgNo		= Convert.ToInt64(parts[1]); break; // 30
						case EBase.__TAG_30607: eICI.CurrentMsgNo	= Convert.ToInt64(parts[1]); break; // 29
						case EBase.__TAG_55   : eICI.Symbol         = parts[1]; break; // isin
                        case EBase.__TAG_30569: eICI.MarketIndexClass = parts[1]; break; // 2
                        case EBase.__TAG_30632: eICI.IdxName = parts[1]; break; // HNX Composite Index
                        case EBase.__TAG_30633: eICI.IdxEnglishName = parts[1]; break; // HNX Composite Index
                    }
                }
            }

            return eICI;
        }

        // ===================================================================================================


        /// <summary>
        /// 2021-03-24 10:18:15 ngocta2
        /// update dic thong tin gia 
        /// 1 exchange gom nhieu thi truong
        /// 1 market gom nhieu board
        /// 1 board gom nhieu quote
        /// 
        /// newQuote.ISIN = VN2STOST0106
        /// </summary>
        /// <param name="quoteDic"></param>
        /// <param name="marketID">STO</param>
        /// <param name="boardID">G1</param>
        /// <param name="newQuote"></param>
        /// <returns></returns>
        public bool UpdateDicQuote(
            ref ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<string, EXQuote>>> quoteDic,
            string marketID,
            string boardID,
            EXQuote newQuote)
        {
            ConcurrentDictionary<string, ConcurrentDictionary<string, EXQuote>> currentMarketDic;
            ConcurrentDictionary<string, EXQuote> currentBoardDic;
            EXQuote currentQuote;

            // msg MF ko co BoardID
            if (boardID == null)
                boardID = EGlobalConfig.__STRING_UNDERSCORE; // _

            // kiem tra market da ton tai chua
            quoteDic.TryGetValue(marketID, out currentMarketDic);

            if (currentMarketDic == null)
            {
                // market chua ton tai
                currentMarketDic = new ConcurrentDictionary<string, ConcurrentDictionary<string, EXQuote>>();
                currentBoardDic = new ConcurrentDictionary<string, EXQuote>();
                newQuote = newQuote.InitZero(ref newQuote); // init tat ca value moi cot la 0, ko init value -1
                currentBoardDic.TryAdd(newQuote.ISIN, newQuote);
                currentMarketDic.TryAdd(boardID, currentBoardDic);
                quoteDic.TryAdd(marketID, currentMarketDic);
                return true;
            }
            else
            {
                // market da ton tai
                // kiem tra board da ton tai chua
                currentMarketDic.TryGetValue(boardID, out currentBoardDic);
                if (currentBoardDic == null)
                {
                    // board chua ton tai
                    currentBoardDic = new ConcurrentDictionary<string, EXQuote>();
                    currentBoardDic.TryAdd(newQuote.ISIN, newQuote);
                    quoteDic[marketID].TryAdd(boardID, currentBoardDic);
                    return true;
                }
                else
                {
                    // board da ton tai
                    // kiem tra quote da ton tai chua
                    currentBoardDic.TryGetValue(newQuote.ISIN, out currentQuote);
                    if (currentQuote == null)
                    {
                        // chua ton tai quote thi them quote vao board dic						
                        quoteDic[marketID][boardID].TryAdd(newQuote.ISIN, newQuote);
                        return true;
                    }
                    else
                    {
                        // da ton tai quote thi update quote
                        quoteDic[marketID][boardID][currentQuote.ISIN] = UpdateXQuoteByCellSeqNum(currentQuote, newQuote);
                        return true;
                    }
                }
            }
        }


        /// <summary>
        /// 2021-03-24 14:27:40 ngocta2
        /// so sanh XQuote hien tai va XQuote moi, so sanh theo CellSeqNum
        /// </summary>
        /// <param name="cQ">currentQuote</param>
        /// <param name="nQ">newQuote</param>
        /// <returns></returns>
        public EXQuote UpdateXQuoteByCellSeqNum(EXQuote cQ, EXQuote nQ)
        {
            string filelog = @"D:\WebLog\S6G\HSXFeederApp\Static\UpdateXQuoteByCellSeqNum\" + cQ.Symbol + ".txt";
            CommonLib.Implementations.CCommon.WriteFileStatic(filelog, $"{EGlobalConfig.DateTimeNow} => cQ={Newtonsoft.Json.JsonConvert.SerializeObject(cQ)}; nQ={Newtonsoft.Json.JsonConvert.SerializeObject(nQ)}");

			if(cQ.SymbolSeqNum                  < nQ.SymbolSeqNum                  ) {cQ.SymbolSeqNum                  = nQ.SymbolSeqNum;                  cQ.Symbol                  = nQ.Symbol;}
			if(cQ.MarketIDSeqNum                < nQ.MarketIDSeqNum                ) {cQ.MarketIDSeqNum                = nQ.MarketIDSeqNum;                cQ.MarketID                = nQ.MarketID;}
			if(cQ.BoardIDSeqNum                 < nQ.BoardIDSeqNum                 ) {cQ.BoardIDSeqNum                 = nQ.BoardIDSeqNum;                 cQ.BoardID                 = nQ.BoardID;}

			if(cQ.ReferencePriceSeqNum          < nQ.ReferencePriceSeqNum          && cQ.ReferencePrice          != nQ.ReferencePrice          && nQ.ReferencePrice          != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.ReferencePriceSeqNum          = nQ.ReferencePriceSeqNum;           cQ.ReferencePrice          = nQ.ReferencePrice;} // neu la init value thi cung ko dc gan. xay ra case nay khi doc msg d co tran,san,tc roi >> sau do doc msg mw, ko co tran,san,tc thi ko de init value vao lai so chuan 
			if(cQ.CeilingPriceSeqNum            < nQ.CeilingPriceSeqNum            && cQ.CeilingPrice            != nQ.CeilingPrice            && nQ.CeilingPrice            != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.CeilingPriceSeqNum            = nQ.CeilingPriceSeqNum;             cQ.CeilingPrice            = nQ.CeilingPrice;}
			if(cQ.FloorPriceSeqNum              < nQ.FloorPriceSeqNum              && cQ.FloorPrice              != nQ.FloorPrice              && nQ.FloorPrice              != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.FloorPriceSeqNum              = nQ.FloorPriceSeqNum;               cQ.FloorPrice              = nQ.FloorPrice;}
			if(cQ.BidCountSeqNum                < nQ.BidCountSeqNum                && cQ.BidCount                != nQ.BidCount                && nQ.BidCount                != EGlobalConfig.__INIT_NULL_LONG)     {cQ.BidCountSeqNum                = nQ.BidCountSeqNum;                 cQ.BidCount                = nQ.BidCount;}
			if(cQ.TotalBidQttySeqNum            < nQ.TotalBidQttySeqNum            && cQ.TotalBidQtty            != nQ.TotalBidQtty            && nQ.TotalBidQtty            != EGlobalConfig.__INIT_NULL_LONG)     {cQ.TotalBidQttySeqNum            = nQ.TotalBidQttySeqNum;             cQ.TotalBidQtty            = nQ.TotalBidQtty;}
			if(cQ.BuyPriceXSeqNum               < nQ.BuyPriceXSeqNum               && cQ.BuyPriceX               != nQ.BuyPriceX               && nQ.BuyPriceX               != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.BuyPriceXSeqNum               = nQ.BuyPriceXSeqNum;                cQ.BuyPriceX               = nQ.BuyPriceX;}
			if(cQ.BuyQuantityXSeqNum            < nQ.BuyQuantityXSeqNum            && cQ.BuyQuantityX            != nQ.BuyQuantityX            && nQ.BuyQuantityX            != EGlobalConfig.__INIT_NULL_LONG)     {cQ.BuyQuantityXSeqNum            = nQ.BuyQuantityXSeqNum;             cQ.BuyQuantityX            = nQ.BuyQuantityX;}
			if(cQ.BuyPrice9SeqNum               < nQ.BuyPrice9SeqNum               && cQ.BuyPrice9               != nQ.BuyPrice9               && nQ.BuyPrice9               != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.BuyPrice9SeqNum               = nQ.BuyPrice9SeqNum;                cQ.BuyPrice9               = nQ.BuyPrice9;}
			if(cQ.BuyQuantity9SeqNum            < nQ.BuyQuantity9SeqNum            && cQ.BuyQuantity9            != nQ.BuyQuantity9            && nQ.BuyQuantity9            != EGlobalConfig.__INIT_NULL_LONG)     {cQ.BuyQuantity9SeqNum            = nQ.BuyQuantity9SeqNum;             cQ.BuyQuantity9            = nQ.BuyQuantity9;}
			if(cQ.BuyPrice8SeqNum               < nQ.BuyPrice8SeqNum               && cQ.BuyPrice8               != nQ.BuyPrice8               && nQ.BuyPrice8               != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.BuyPrice8SeqNum               = nQ.BuyPrice8SeqNum;                cQ.BuyPrice8               = nQ.BuyPrice8;}
			if(cQ.BuyQuantity8SeqNum            < nQ.BuyQuantity8SeqNum            && cQ.BuyQuantity8            != nQ.BuyQuantity8            && nQ.BuyQuantity8            != EGlobalConfig.__INIT_NULL_LONG)     {cQ.BuyQuantity8SeqNum            = nQ.BuyQuantity8SeqNum;             cQ.BuyQuantity8            = nQ.BuyQuantity8;}
			if(cQ.BuyPrice7SeqNum               < nQ.BuyPrice7SeqNum               && cQ.BuyPrice7               != nQ.BuyPrice7               && nQ.BuyPrice7               != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.BuyPrice7SeqNum               = nQ.BuyPrice7SeqNum;                cQ.BuyPrice7               = nQ.BuyPrice7;}
			if(cQ.BuyQuantity7SeqNum            < nQ.BuyQuantity7SeqNum            && cQ.BuyQuantity7            != nQ.BuyQuantity7            && nQ.BuyQuantity7            != EGlobalConfig.__INIT_NULL_LONG)     {cQ.BuyQuantity7SeqNum            = nQ.BuyQuantity7SeqNum;             cQ.BuyQuantity7            = nQ.BuyQuantity7;}
			if(cQ.BuyPrice6SeqNum               < nQ.BuyPrice6SeqNum               && cQ.BuyPrice6               != nQ.BuyPrice6               && nQ.BuyPrice6               != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.BuyPrice6SeqNum               = nQ.BuyPrice6SeqNum;                cQ.BuyPrice6               = nQ.BuyPrice6;}
			if(cQ.BuyQuantity6SeqNum            < nQ.BuyQuantity6SeqNum            && cQ.BuyQuantity6            != nQ.BuyQuantity6            && nQ.BuyQuantity6            != EGlobalConfig.__INIT_NULL_LONG)     {cQ.BuyQuantity6SeqNum            = nQ.BuyQuantity6SeqNum;             cQ.BuyQuantity6            = nQ.BuyQuantity6;}
			if(cQ.BuyPrice5SeqNum               < nQ.BuyPrice5SeqNum               && cQ.BuyPrice5               != nQ.BuyPrice5               && nQ.BuyPrice5               != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.BuyPrice5SeqNum               = nQ.BuyPrice5SeqNum;                cQ.BuyPrice5               = nQ.BuyPrice5;}
			if(cQ.BuyQuantity5SeqNum            < nQ.BuyQuantity5SeqNum            && cQ.BuyQuantity5            != nQ.BuyQuantity5            && nQ.BuyQuantity5            != EGlobalConfig.__INIT_NULL_LONG)     {cQ.BuyQuantity5SeqNum            = nQ.BuyQuantity5SeqNum;             cQ.BuyQuantity5            = nQ.BuyQuantity5;}
			if(cQ.BuyPrice4SeqNum               < nQ.BuyPrice4SeqNum               && cQ.BuyPrice4               != nQ.BuyPrice4               && nQ.BuyPrice4               != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.BuyPrice4SeqNum               = nQ.BuyPrice4SeqNum;                cQ.BuyPrice4               = nQ.BuyPrice4;}
			if(cQ.BuyQuantity4SeqNum            < nQ.BuyQuantity4SeqNum            && cQ.BuyQuantity4            != nQ.BuyQuantity4            && nQ.BuyQuantity4            != EGlobalConfig.__INIT_NULL_LONG)     {cQ.BuyQuantity4SeqNum            = nQ.BuyQuantity4SeqNum;             cQ.BuyQuantity4            = nQ.BuyQuantity4;}
			if(cQ.BuyPrice3SeqNum               < nQ.BuyPrice3SeqNum               && cQ.BuyPrice3               != nQ.BuyPrice3               && nQ.BuyPrice3               != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.BuyPrice3SeqNum               = nQ.BuyPrice3SeqNum;                cQ.BuyPrice3               = nQ.BuyPrice3;}
			if(cQ.BuyQuantity3SeqNum            < nQ.BuyQuantity3SeqNum            && cQ.BuyQuantity3            != nQ.BuyQuantity3            && nQ.BuyQuantity3            != EGlobalConfig.__INIT_NULL_LONG)     {cQ.BuyQuantity3SeqNum            = nQ.BuyQuantity3SeqNum;             cQ.BuyQuantity3            = nQ.BuyQuantity3;}
			if(cQ.BuyPrice2SeqNum               < nQ.BuyPrice2SeqNum               && cQ.BuyPrice2               != nQ.BuyPrice2               && nQ.BuyPrice2               != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.BuyPrice2SeqNum               = nQ.BuyPrice2SeqNum;                cQ.BuyPrice2               = nQ.BuyPrice2;}
			if(cQ.BuyQuantity2SeqNum            < nQ.BuyQuantity2SeqNum            && cQ.BuyQuantity2            != nQ.BuyQuantity2            && nQ.BuyQuantity2            != EGlobalConfig.__INIT_NULL_LONG)     {cQ.BuyQuantity2SeqNum            = nQ.BuyQuantity2SeqNum;             cQ.BuyQuantity2            = nQ.BuyQuantity2;}
			if(cQ.BuyPrice1SeqNum               < nQ.BuyPrice1SeqNum               && cQ.BuyPrice1               != nQ.BuyPrice1               && nQ.BuyPrice1               != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.BuyPrice1SeqNum               = nQ.BuyPrice1SeqNum;                cQ.BuyPrice1               = nQ.BuyPrice1;}
			if(cQ.BuyQuantity1SeqNum            < nQ.BuyQuantity1SeqNum            && cQ.BuyQuantity1            != nQ.BuyQuantity1            && nQ.BuyQuantity1            != EGlobalConfig.__INIT_NULL_LONG)     {cQ.BuyQuantity1SeqNum            = nQ.BuyQuantity1SeqNum;             cQ.BuyQuantity1            = nQ.BuyQuantity1;}
			if(cQ.MatchPriceSeqNum              < nQ.MatchPriceSeqNum              && cQ.MatchPrice              != nQ.MatchPrice              && nQ.MatchPrice              != EGlobalConfig.__INIT_NULL_LONG)     {cQ.MatchPriceSeqNum              = nQ.MatchPriceSeqNum;               cQ.MatchPrice              = nQ.MatchPrice;}
			if(cQ.MatchQuantitySeqNum           < nQ.MatchQuantitySeqNum           && cQ.MatchQuantity           != nQ.MatchQuantity           && nQ.MatchQuantity           != EGlobalConfig.__INIT_NULL_LONG)     {cQ.MatchQuantitySeqNum           = nQ.MatchQuantitySeqNum;            cQ.MatchQuantity           = nQ.MatchQuantity;}
			if(cQ.MatchChangeSeqNum             < nQ.MatchChangeSeqNum             && cQ.MatchChange             != nQ.MatchChange             && nQ.MatchChange             != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.MatchChangeSeqNum             = nQ.MatchChangeSeqNum;              cQ.MatchChange             = nQ.MatchChange;}
			if(cQ.MatchChangePercentSeqNum      < nQ.MatchChangePercentSeqNum      && cQ.MatchChangePercent      != nQ.MatchChangePercent      && nQ.MatchChangePercent      != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.MatchChangePercentSeqNum      = nQ.MatchChangePercentSeqNum;       cQ.MatchChangePercent      = nQ.MatchChangePercent;}
			if(cQ.TotalNMQuantitySeqNum         < nQ.TotalNMQuantitySeqNum         && cQ.TotalNMQuantity         != nQ.TotalNMQuantity         && nQ.TotalNMQuantity         != EGlobalConfig.__INIT_NULL_LONG)     {cQ.TotalNMQuantitySeqNum         = nQ.TotalNMQuantitySeqNum;          cQ.TotalNMQuantity         = nQ.TotalNMQuantity;}
			if(cQ.SellPrice1SeqNum              < nQ.SellPrice1SeqNum              && cQ.SellPrice1              != nQ.SellPrice1              && nQ.SellPrice1              != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.SellPrice1SeqNum              = nQ.SellPrice1SeqNum;               cQ.SellPrice1              = nQ.SellPrice1;}
			if(cQ.SellQuantity1SeqNum           < nQ.SellQuantity1SeqNum           && cQ.SellQuantity1           != nQ.SellQuantity1           && nQ.SellQuantity1           != EGlobalConfig.__INIT_NULL_LONG)     {cQ.SellQuantity1SeqNum           = nQ.SellQuantity1SeqNum;            cQ.SellQuantity1           = nQ.SellQuantity1;}
			if(cQ.SellPrice2SeqNum              < nQ.SellPrice2SeqNum              && cQ.SellPrice2              != nQ.SellPrice2              && nQ.SellPrice2              != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.SellPrice2SeqNum              = nQ.SellPrice2SeqNum;               cQ.SellPrice2              = nQ.SellPrice2;}
			if(cQ.SellQuantity2SeqNum           < nQ.SellQuantity2SeqNum           && cQ.SellQuantity2           != nQ.SellQuantity2           && nQ.SellQuantity2           != EGlobalConfig.__INIT_NULL_LONG)     {cQ.SellQuantity2SeqNum           = nQ.SellQuantity2SeqNum;            cQ.SellQuantity2           = nQ.SellQuantity2;}
			if(cQ.SellPrice3SeqNum              < nQ.SellPrice3SeqNum              && cQ.SellPrice3              != nQ.SellPrice3              && nQ.SellPrice3              != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.SellPrice3SeqNum              = nQ.SellPrice3SeqNum;               cQ.SellPrice3              = nQ.SellPrice3;}
			if(cQ.SellQuantity3SeqNum           < nQ.SellQuantity3SeqNum           && cQ.SellQuantity3           != nQ.SellQuantity3           && nQ.SellQuantity3           != EGlobalConfig.__INIT_NULL_LONG)     {cQ.SellQuantity3SeqNum           = nQ.SellQuantity3SeqNum;            cQ.SellQuantity3           = nQ.SellQuantity3;}
			if(cQ.SellPrice4SeqNum              < nQ.SellPrice4SeqNum              && cQ.SellPrice4              != nQ.SellPrice4              && nQ.SellPrice4              != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.SellPrice4SeqNum              = nQ.SellPrice4SeqNum;               cQ.SellPrice4              = nQ.SellPrice4;}
			if(cQ.SellQuantity4SeqNum           < nQ.SellQuantity4SeqNum           && cQ.SellQuantity4           != nQ.SellQuantity4           && nQ.SellQuantity4           != EGlobalConfig.__INIT_NULL_LONG)     {cQ.SellQuantity4SeqNum           = nQ.SellQuantity4SeqNum;            cQ.SellQuantity4           = nQ.SellQuantity4;}
			if(cQ.SellPrice5SeqNum              < nQ.SellPrice5SeqNum              && cQ.SellPrice5              != nQ.SellPrice5              && nQ.SellPrice5              != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.SellPrice5SeqNum              = nQ.SellPrice5SeqNum;               cQ.SellPrice5              = nQ.SellPrice5;}
			if(cQ.SellQuantity5SeqNum           < nQ.SellQuantity5SeqNum           && cQ.SellQuantity5           != nQ.SellQuantity5           && nQ.SellQuantity5           != EGlobalConfig.__INIT_NULL_LONG)     {cQ.SellQuantity5SeqNum           = nQ.SellQuantity5SeqNum;            cQ.SellQuantity5           = nQ.SellQuantity5;}
			if(cQ.SellPrice6SeqNum              < nQ.SellPrice6SeqNum              && cQ.SellPrice6              != nQ.SellPrice6              && nQ.SellPrice6              != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.SellPrice6SeqNum              = nQ.SellPrice6SeqNum;               cQ.SellPrice6              = nQ.SellPrice6;}
			if(cQ.SellQuantity6SeqNum           < nQ.SellQuantity6SeqNum           && cQ.SellQuantity6           != nQ.SellQuantity6           && nQ.SellQuantity6           != EGlobalConfig.__INIT_NULL_LONG)     {cQ.SellQuantity6SeqNum           = nQ.SellQuantity6SeqNum;            cQ.SellQuantity6           = nQ.SellQuantity6;}
			if(cQ.SellPrice7SeqNum              < nQ.SellPrice7SeqNum              && cQ.SellPrice7              != nQ.SellPrice7              && nQ.SellPrice7              != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.SellPrice7SeqNum              = nQ.SellPrice7SeqNum;               cQ.SellPrice7              = nQ.SellPrice7;}
			if(cQ.SellQuantity7SeqNum           < nQ.SellQuantity7SeqNum           && cQ.SellQuantity7           != nQ.SellQuantity7           && nQ.SellQuantity7           != EGlobalConfig.__INIT_NULL_LONG)     {cQ.SellQuantity7SeqNum           = nQ.SellQuantity7SeqNum;            cQ.SellQuantity7           = nQ.SellQuantity7;}
			if(cQ.SellPrice8SeqNum              < nQ.SellPrice8SeqNum              && cQ.SellPrice8              != nQ.SellPrice8              && nQ.SellPrice8              != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.SellPrice8SeqNum              = nQ.SellPrice8SeqNum;               cQ.SellPrice8              = nQ.SellPrice8;}
			if(cQ.SellQuantity8SeqNum           < nQ.SellQuantity8SeqNum           && cQ.SellQuantity8           != nQ.SellQuantity8           && nQ.SellQuantity8           != EGlobalConfig.__INIT_NULL_LONG)     {cQ.SellQuantity8SeqNum           = nQ.SellQuantity8SeqNum;            cQ.SellQuantity8           = nQ.SellQuantity8;}
			if(cQ.SellPrice9SeqNum              < nQ.SellPrice9SeqNum              && cQ.SellPrice9              != nQ.SellPrice9              && nQ.SellPrice9              != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.SellPrice9SeqNum              = nQ.SellPrice9SeqNum;               cQ.SellPrice9              = nQ.SellPrice9;}
			if(cQ.SellQuantity9SeqNum           < nQ.SellQuantity9SeqNum           && cQ.SellQuantity9           != nQ.SellQuantity9           && nQ.SellQuantity9           != EGlobalConfig.__INIT_NULL_LONG)     {cQ.SellQuantity9SeqNum           = nQ.SellQuantity9SeqNum;            cQ.SellQuantity9           = nQ.SellQuantity9;}
			if(cQ.SellPriceXSeqNum              < nQ.SellPriceXSeqNum              && cQ.SellPriceX              != nQ.SellPriceX              && nQ.SellPriceX              != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.SellPriceXSeqNum              = nQ.SellPriceXSeqNum;               cQ.SellPriceX              = nQ.SellPriceX;}
			if(cQ.SellQuantityXSeqNum           < nQ.SellQuantityXSeqNum           && cQ.SellQuantityX           != nQ.SellQuantityX           && nQ.SellQuantityX           != EGlobalConfig.__INIT_NULL_LONG)     {cQ.SellQuantityXSeqNum           = nQ.SellQuantityXSeqNum;            cQ.SellQuantityX           = nQ.SellQuantityX;}
			if(cQ.OfferCountSeqNum              < nQ.OfferCountSeqNum              && cQ.OfferCount              != nQ.OfferCount              && nQ.OfferCount              != EGlobalConfig.__INIT_NULL_LONG)     {cQ.OfferCountSeqNum              = nQ.OfferCountSeqNum;               cQ.OfferCount              = nQ.OfferCount;}
			if(cQ.TotalOfferQttySeqNum          < nQ.TotalOfferQttySeqNum          && cQ.TotalOfferQtty          != nQ.TotalOfferQtty          && nQ.TotalOfferQtty          != EGlobalConfig.__INIT_NULL_LONG)     {cQ.TotalOfferQttySeqNum          = nQ.TotalOfferQttySeqNum;           cQ.TotalOfferQtty          = nQ.TotalOfferQtty;}
			if(cQ.OpenPriceSeqNum               < nQ.OpenPriceSeqNum               && cQ.OpenPrice               != nQ.OpenPrice               && nQ.OpenPrice               != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.OpenPriceSeqNum               = nQ.OpenPriceSeqNum;                cQ.OpenPrice               = nQ.OpenPrice;}
			if(cQ.AveragePriceSeqNum            < nQ.AveragePriceSeqNum            && cQ.AveragePrice            != nQ.AveragePrice            && nQ.AveragePrice            != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.AveragePriceSeqNum            = nQ.AveragePriceSeqNum;             cQ.AveragePrice            = nQ.AveragePrice;}
			if(cQ.HighestPriceSeqNum            < nQ.HighestPriceSeqNum            && cQ.HighestPrice            != nQ.HighestPrice            && nQ.HighestPrice            != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.HighestPriceSeqNum            = nQ.HighestPriceSeqNum;             cQ.HighestPrice            = nQ.HighestPrice;}
			if(cQ.LowestPriceSeqNum             < nQ.LowestPriceSeqNum             && cQ.LowestPrice             != nQ.LowestPrice             && nQ.LowestPrice             != EGlobalConfig.__INIT_NULL_DOUBLE)   {cQ.LowestPriceSeqNum             = nQ.LowestPriceSeqNum;              cQ.LowestPrice             = nQ.LowestPrice;}
			if(cQ.ForeignBuyQuantitySeqNum      < nQ.ForeignBuyQuantitySeqNum      && cQ.ForeignBuyQuantity      != nQ.ForeignBuyQuantity      && nQ.ForeignBuyQuantity      != EGlobalConfig.__INIT_NULL_LONG)     {cQ.ForeignBuyQuantitySeqNum      = nQ.ForeignBuyQuantitySeqNum;       cQ.ForeignBuyQuantity      = nQ.ForeignBuyQuantity;}
			if(cQ.ForeignSellQuantitySeqNum     < nQ.ForeignSellQuantitySeqNum     && cQ.ForeignSellQuantity     != nQ.ForeignSellQuantity     && nQ.ForeignSellQuantity     != EGlobalConfig.__INIT_NULL_LONG)     {cQ.ForeignSellQuantitySeqNum     = nQ.ForeignSellQuantitySeqNum;      cQ.ForeignSellQuantity     = nQ.ForeignSellQuantity;}
			if(cQ.ForeignRoomRemainSeqNum       < nQ.ForeignRoomRemainSeqNum       && cQ.ForeignRoomRemain       != nQ.ForeignRoomRemain       && nQ.ForeignRoomRemain       != EGlobalConfig.__INIT_NULL_LONG)     {cQ.ForeignRoomRemainSeqNum       = nQ.ForeignRoomRemainSeqNum;        cQ.ForeignRoomRemain       = nQ.ForeignRoomRemain;}
			if(cQ.OpenInterestSeqNum            < nQ.OpenInterestSeqNum            && cQ.OpenInterest            != nQ.OpenInterest            && nQ.OpenInterest            != EGlobalConfig.__INIT_NULL_LONG)     {cQ.OpenInterestSeqNum            = nQ.OpenInterestSeqNum;             cQ.OpenInterest            = nQ.OpenInterest;}
			if(cQ.LastTradingDateSeqNum         < nQ.LastTradingDateSeqNum         && cQ.LastTradingDate         != nQ.LastTradingDate         && nQ.LastTradingDate         != EGlobalConfig.__INIT_NULL_STRING)   {cQ.LastTradingDateSeqNum         = nQ.LastTradingDateSeqNum;          cQ.LastTradingDate         = nQ.LastTradingDate;}
			if(cQ.ExClassTypeSeqNum             < nQ.ExClassTypeSeqNum             && cQ.ExClassType             != nQ.ExClassType             && nQ.ExClassType             != EGlobalConfig.__INIT_NULL_STRING)   {cQ.ExClassTypeSeqNum             = nQ.ExClassTypeSeqNum;              cQ.ExClassType             = nQ.ExClassType;}
			if(cQ.SymbolTradingMethodSeqNum     < nQ.SymbolTradingMethodSeqNum     && cQ.SymbolTradingMethod     != nQ.SymbolTradingMethod     && nQ.SymbolTradingMethod     != EGlobalConfig.__INIT_NULL_STRING)   {cQ.SymbolTradingMethodSeqNum     = nQ.SymbolTradingMethodSeqNum;      cQ.SymbolTradingMethod     = nQ.SymbolTradingMethod;}
			if(cQ.SymbolTradingSantionSeqNum    < nQ.SymbolTradingSantionSeqNum    && cQ.SymbolTradingSantion    != nQ.SymbolTradingSantion    && nQ.SymbolTradingSantion    != EGlobalConfig.__INIT_NULL_STRING)   {cQ.SymbolTradingSantionSeqNum    = nQ.SymbolTradingSantionSeqNum;     cQ.SymbolTradingSantion    = nQ.SymbolTradingSantion;}
			if(cQ.TentativeExecutionPriceSeqNum < nQ.TentativeExecutionPriceSeqNum && cQ.TentativeExecutionPrice != nQ.TentativeExecutionPrice && nQ.TentativeExecutionPrice != EGlobalConfig.__INIT_NULL_LONG)     {cQ.TentativeExecutionPriceSeqNum = nQ.TentativeExecutionPriceSeqNum;  cQ.TentativeExecutionPrice = nQ.TentativeExecutionPrice;}
			if(cQ.ExpectedTradePxSeqNum         < nQ.ExpectedTradePxSeqNum         && cQ.ExpectedTradePx         != nQ.ExpectedTradePx         && nQ.ExpectedTradePx         != EGlobalConfig.__INIT_NULL_LONG)     {cQ.ExpectedTradePxSeqNum         = nQ.ExpectedTradePxSeqNum;          cQ.ExpectedTradePx         = nQ.ExpectedTradePx;}
			if(cQ.ExpectedTradeQtySeqNum        < nQ.ExpectedTradeQtySeqNum        && cQ.ExpectedTradeQty        != nQ.ExpectedTradeQty        && nQ.ExpectedTradeQty        != EGlobalConfig.__INIT_NULL_LONG)     {cQ.ExpectedTradeQtySeqNum        = nQ.ExpectedTradeQtySeqNum;         cQ.ExpectedTradeQty        = nQ.ExpectedTradeQty;}
			if(cQ.BoardEvtIDSeqNum              < nQ.BoardEvtIDSeqNum              && cQ.BoardEvtID              != nQ.BoardEvtID              && nQ.BoardEvtID              != EGlobalConfig.__INIT_NULL_STRING)   {cQ.BoardEvtIDSeqNum              = nQ.BoardEvtIDSeqNum;               cQ.BoardEvtID              = nQ.BoardEvtID;}
			if(cQ.SessOpenCloseCodeSeqNum       < nQ.SessOpenCloseCodeSeqNum       && cQ.SessOpenCloseCode       != nQ.SessOpenCloseCode       && nQ.SessOpenCloseCode       != EGlobalConfig.__INIT_NULL_STRING)   {cQ.SessOpenCloseCodeSeqNum       = nQ.SessOpenCloseCodeSeqNum;        cQ.SessOpenCloseCode       = nQ.SessOpenCloseCode;}
			if(cQ.TradingSessionIDSeqNum        < nQ.TradingSessionIDSeqNum        && cQ.TradingSessionID        != nQ.TradingSessionID        && nQ.TradingSessionID        != EGlobalConfig.__INIT_NULL_STRING)   {cQ.TradingSessionIDSeqNum        = nQ.TradingSessionIDSeqNum;         cQ.TradingSessionID        = nQ.TradingSessionID;}
			if(cQ.TransactTimeSeqNum            < nQ.TransactTimeSeqNum            && cQ.TransactTime            != nQ.TransactTime            && nQ.TransactTime            != EGlobalConfig.__INIT_NULL_STRING)   {cQ.TransactTimeSeqNum            = nQ.TransactTimeSeqNum;             cQ.TransactTime            = nQ.TransactTime;}
			if(cQ.TradeDateSeqNum               < nQ.TradeDateSeqNum               && cQ.TradeDate               != nQ.TradeDate               && nQ.TradeDate               != EGlobalConfig.__INIT_NULL_STRING)   {cQ.TradeDateSeqNum               = nQ.TradeDateSeqNum;                cQ.TradeDate               = nQ.TradeDate;}

            return cQ;
        }

        /// <summary>
        /// 2021-03-24 16:05:53 ngocta2
        /// convert MDDS Price (X) thanh JsonX Quote
        /// </summary>
        /// <param name="x"></param>
        /// <param name="d"></param>
        /// <returns></returns>
        public EXQuote Convert_X2XQuote(EPrice x, ESecurityDefinition d)
        {
            // tao new instance
            EXQuote xQuote = new EXQuote(x.MsgSeqNum); // 2021-04-28 15:57:42 ngocta2

            //update dic key			
			xQuote.MarketID               = x.MarketID;
			xQuote.BoardID                = x.BoardID;
			xQuote.ISIN					  = x.Symbol;
			xQuote.Symbol				  = d==null?x.Symbol:d.TickerCode; // 2021-07-21 14:50:12 ngocta2

            // 2021-11-02 10:35:26 ngocta2 >> tran,san,tc phai lay tu d => msg d ve truoc msg X
			if(d!=null)
            {
                // 2022-03-15 disable code de ss quote ko lien tuc send lai tran,san,tc giong nhau
                //xQuote.ReferencePrice	  = d.ReferencePrice;
                //xQuote.CeilingPrice		  = d.HighLimitPrice;
                //xQuote.FloorPrice		  = d.LowLimitPrice;
            }

            //update data price
            xQuote.BidCount               = x.BuyValidOrderCnt;
			xQuote.TotalBidQtty           = x.BuyTotOrderQty;
			xQuote.BuyPriceX              = x.BuyPrice10;
			xQuote.BuyQuantityX           = x.BuyQuantity10;
			xQuote.BuyPrice9              = x.BuyPrice9;
			xQuote.BuyQuantity9           = x.BuyQuantity9;
			xQuote.BuyPrice8              = x.BuyPrice8;
			xQuote.BuyQuantity8           = x.BuyQuantity8;
			xQuote.BuyPrice7              = x.BuyPrice7;
			xQuote.BuyQuantity7           = x.BuyQuantity7;
			xQuote.BuyPrice6              = x.BuyPrice6;
			xQuote.BuyQuantity6           = x.BuyQuantity6;
			xQuote.BuyPrice5              = x.BuyPrice5;
			xQuote.BuyQuantity5           = x.BuyQuantity5;
			xQuote.BuyPrice4              = x.BuyPrice4;
			xQuote.BuyQuantity4           = x.BuyQuantity4;
			xQuote.BuyPrice3              = x.BuyPrice3;
			xQuote.BuyQuantity3           = x.BuyQuantity3;
			xQuote.BuyPrice2              = x.BuyPrice2;
			xQuote.BuyQuantity2           = x.BuyQuantity2;
			xQuote.BuyPrice1              = x.BuyPrice1;
			xQuote.BuyQuantity1           = x.BuyQuantity1;
			xQuote.MatchPrice             = x.MatchPrice;
			xQuote.MatchQuantity          = x.MatchQuantity;
            //xQuote.MatchChange          = d.MatchChange,        // <tự tính>
            //xQuote.MatchChangePercent   = d.MatchChangePercent, // <tự tính>
			if (xQuote.MatchPrice         > 0 && xQuote.ReferencePrice > 0)
            {
				xQuote.MatchChange        = xQuote.MatchPrice - xQuote.ReferencePrice;
                xQuote.MatchChangePercent = (xQuote.MatchChange / xQuote.ReferencePrice) * 100;
            }
			xQuote.TotalNMQuantity        = x.TotalVolumeTraded;
			xQuote.SellPrice1             = x.SellPrice1;
			xQuote.SellQuantity1          = x.SellQuantity1;
			xQuote.SellPrice2             = x.SellPrice2;
			xQuote.SellQuantity2          = x.SellQuantity2;
			xQuote.SellPrice3             = x.SellPrice3;
			xQuote.SellQuantity3          = x.SellQuantity3;
			xQuote.SellPrice4             = x.SellPrice4;
			xQuote.SellQuantity4          = x.SellQuantity4;
			xQuote.SellPrice5             = x.SellPrice5;
			xQuote.SellQuantity5          = x.SellQuantity5;
			xQuote.SellPrice6             = x.SellPrice6;
			xQuote.SellQuantity6          = x.SellQuantity6;
			xQuote.SellPrice7             = x.SellPrice7;
			xQuote.SellQuantity7          = x.SellQuantity7;
			xQuote.SellPrice8             = x.SellPrice8;
			xQuote.SellQuantity8          = x.SellQuantity8;
			xQuote.SellPrice9             = x.SellPrice9;
			xQuote.SellQuantity9          = x.SellQuantity9;
			xQuote.SellPriceX             = x.SellPrice10;
			xQuote.SellQuantityX          = x.SellQuantity10;
			xQuote.OfferCount             = x.SellValidOrderCnt;
			xQuote.TotalOfferQtty         = x.SellTotOrderQty;
			xQuote.OpenPrice              = x.OpenPrice;
			xQuote.HighestPrice           = x.HighestPrice;
			xQuote.LowestPrice            = x.LowestPrice;
			xQuote.TransactTime			  = x.TransactTime; // 2021-10-21 10:48:04 ngocta2
			xQuote.TradeDate			  = x.TradeDate;    // 2021-10-22 14:08:12 ngocta2

			xQuote.TradingSessionID		  = x.TradingSessionID;// 2022-03-16 ngocta2; can them tinh trang rieng cua tung ma de nhay dung ATO/ATC do tinh trang chung cua index thuong ve sau

            // return
            return xQuote;
        }

        /// <summary>
        /// 2021-03-29 14:39:52 ngocta2
        /// convert MDDS SecurityDefinition (d) thanh JsonX Quote
        /// </summary>
        /// <param name="d"></param>
        /// <returns></returns>
        public EXQuote Convert_d2XQuote(ESecurityDefinition d)
        {
            // tao new instance
            EXQuote xQuote = new EXQuote(d.MsgSeqNum);

            //update dic key			
			xQuote.MarketID               = d.MarketID;
			xQuote.BoardID                = d.BoardID;
			xQuote.ISIN					  = d.Symbol;
            //update data price
			xQuote.Symbol				  = d.TickerCode;
			xQuote.ReferencePrice         = d.ReferencePrice;
			xQuote.CeilingPrice           = d.HighLimitPrice;
			xQuote.FloorPrice             = d.LowLimitPrice;
            //xQuote.MatchChange          = d.MatchChange,        // <tự tính>
            //xQuote.MatchChangePercent   = d.MatchChangePercent, // <tự tính>
			if (xQuote.MatchPrice         > 0 && xQuote.ReferencePrice > 0)
            {
				xQuote.MatchChange        = xQuote.MatchPrice - xQuote.ReferencePrice;			
                xQuote.MatchChangePercent = (xQuote.MatchChange / xQuote.ReferencePrice) * 100;
            }
			//xQuote.OpenInterest           = d.OpenInterestQty;
			//xQuote.LastTradingDate        = d.FinalTradeDate;
			xQuote.ExClassType            = d.ExClassType;
			xQuote.SymbolTradingMethod    = d.SymbolTradingMethodStatusCode;
			xQuote.SymbolTradingSantion   = d.SymbolTradingSantionStatusCode;

            // return
            return xQuote;
        }

        /// <summary>
        /// 2021-03-29 17:22:14 ngocta2
        /// "Giá dự kiến được thực hiện trong phiên khớp lệnh định kỳ 
        /// khi RE được áp dụng(Sự kiện RE xảy ra trong các đợt giao dịch khớp lệnh định kỳ.)"
        /// </summary>
        /// <param name="mw"></param>
        /// <param name="d"></param>
        /// <returns></returns>
        public EXQuote Convert_MW2XQuote(ERandomEnd mw, ESecurityDefinition d)
        {
            // tao new instance
            EXQuote xQuote = new EXQuote(mw.MsgSeqNum);

            //update dic key			
			xQuote.MarketID                = mw.MarketID;
			xQuote.BoardID                 = mw.BoardID;
			xQuote.ISIN                    = mw.Symbol;
			xQuote.Symbol				   = d==null?mw.Symbol:d.TickerCode; // 2021-07-21 14:50:12 ngocta2
                                                                  //update data price
            xQuote.TentativeExecutionPrice = mw.RandomEndTentativeExecutionPrice;

            // return
            return xQuote;
        }

        /// <summary>
        /// 2021-03-30 11:22:43 ngocta2
        /// lay 2 info sau tu msg MT
        /// + KL nước ngoài mua
        /// + KL nước ngoài bán
        /// </summary>
        /// <param name="mt"></param>
        /// <param name="d"></param>
        /// <returns></returns>
        public EXQuote Convert_MT2XQuote(ETradingResultOfForeignInvestors mt, ESecurityDefinition d)
        {
            // tao new instance
            EXQuote xQuote = new EXQuote(mt.MsgSeqNum);

            //update dic key			
			xQuote.MarketID            = mt.MarketID;
			xQuote.BoardID             = mt.BoardID;
			xQuote.ISIN                = mt.Symbol;
			xQuote.Symbol			   = d==null?mt.Symbol:d.TickerCode; // 2021-07-21 14:50:12 ngocta2
                                                                  //update data price
			xQuote.ForeignBuyQuantity  = mt.BuyVolume;
            xQuote.ForeignSellQuantity = mt.SellVolume;

            // return
            return xQuote;
        }

        /// <summary>
        /// 2021-03-30 13:30:26 ngocta2
        /// lay 2 info sau
        /// + Giá dự kiến giao dịch (phiên định kỳ khớp lệnh mở cửa)
        /// + Khối lượng dự kiến giao dịch(phiên định kỳ khớp lệnh mở cửa)
        /// </summary>
        /// <param name="me"></param>
        /// <param name="d"></param>
        /// <returns></returns>
        public EXQuote Convert_ME2XQuote(EDeemTradePrice me, ESecurityDefinition d)
        {
            // tao new instance
            EXQuote xQuote = new EXQuote(me.MsgSeqNum);

            //update dic key			
			xQuote.MarketID         = me.MarketID;
			xQuote.BoardID          = me.BoardID;
			xQuote.ISIN             = me.Symbol;
			xQuote.Symbol			= d==null?me.Symbol:d.TickerCode; // 2021-07-21 14:50:12 ngocta2
                                                                  //update data price
			xQuote.ExpectedTradePx  = me.ExpectedTradePx;
            xQuote.ExpectedTradeQty = me.ExpectedTradeQty;

            // return
            return xQuote;
        }

        /// <summary>
        /// 2021-03-30 13:39:44 ngocta2
        /// lay thong tin sau
        /// ForeignRoomRemain	(KL còn lại được phép mua)
        /// do msg nay ko co BoardID => tim cach map voi BoardID tai msg khac, dung dic
        /// o msg khac MF set dic["VN7139320007"]="G1"
        /// o msg MF get dic["VN7139320007"] de lay ra "G1"
        /// </summary>
        /// <param name="mf"></param>		
        /// <param name="d"></param>
        /// <param name="boardID"></param>
        /// <returns></returns>
        public EXQuote Convert_MF2XQuote(EForeignerOrderLimit mf, ESecurityDefinition d, string boardID)
        {
            // tao new instance
            EXQuote xQuote = new EXQuote(mf.MsgSeqNum);

            //update dic key			
			xQuote.MarketID          = mf.MarketID;
			xQuote.BoardID			 = boardID; // msg nay ko BoardID, lay tu boardID truyen vao
			xQuote.ISIN              = mf.Symbol;
			xQuote.Symbol			 = d==null?mf.Symbol:d.TickerCode; // 2021-07-21 14:50:12 ngocta2
                                                                  //update data price
            xQuote.ForeignRoomRemain = mf.ForeignerOrderLimitQty;

            // return
            return xQuote;
        }

        /// <summary>
        /// 2021-07-21 14:38:36 ngocta2
        /// thong tin tinh trang co phieu, thuong la nhan dc dau tien trong ngay
        /// </summary>
        /// <param name="f"></param>
        /// <param name="d"></param>
        /// <returns></returns>
        public EXQuote Convert_f2XQuote(ESecurityStatus f, ESecurityDefinition d)
        {
            // tao new instance
            EXQuote xQuote = new EXQuote(f.MsgSeqNum);

            //update dic key			
			xQuote.MarketID        = f.MarketID;
			xQuote.BoardID         = f.BoardID; // msg nay ko BoardID
			xQuote.ISIN            = f.Symbol;
			xQuote.Symbol          = d==null?f.Symbol:d.TickerCode; // 2021-07-21 14:50:12 ngocta2
                                                                 //update status
			xQuote.BoardEvtID		 = f.BoardEvtID;
            xQuote.SessOpenCloseCode = f.SessOpenCloseCode;
			xQuote.TradingSessionID  = f.TradingSessionID;

            // return
            return xQuote;
        }

        /// <summary>
        /// 2021-03-31 15:07:38 ngocta2
        /// </summary>
        /// <param name="indexDic"></param>
        /// <param name="exchange"></param>
        /// <returns></returns>
        public bool GetLastIndex(ref ConcurrentDictionary<string, EXIndex> indexDic, string exchange)
        {
            return false;
        }

        /// <summary>
        /// 2021-04-15 14:05:56 ngocta2
        /// chuyen list XQuote sang XQuoteS
        /// XQuoteS bo qua cac field bi null ( ko xerialize )
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public List<EXQuoteS> EXQuoteList2EXQuoteSList(List<EXQuote> list)
        {
            List<EXQuoteS> xQuoteSList = new List<EXQuoteS>();
            foreach (EXQuote xQuote in list)
                xQuoteSList.Add(new EXQuoteS(xQuote));

            return xQuoteSList;
        }

        /// <summary>
        /// 2021-07-23 11:18:07 ngocta2
        /// update danh muc basket
        /// 1 market co nhieu basket
        /// 
        /// 2021-11-19 15:25:03 ngocta2
        /// voi data that cua HNX tra ve thi co 2 ro cung ten, khac market
        /// [UPX][001]="A32,A32,ABC,ABC,ABC,ABI,"
        /// [STX][001]="AAV,AAV,AAV,ACB,ACB,ACB,ACM,ACM,ADC,ADC,"
        /// </summary>
        /// <param name="basketDic"></param>
        /// <param name="ml"></param>
        /// <returns></returns>
        public bool UpdateDicBasket(
            ref ConcurrentDictionary<string, ConcurrentDictionary<string, List<string>>> basketDic,
            EIndexConstituentsInformation ml)
        {
            ConcurrentDictionary<string, List<string>> currentMarketDic;
            List<string> isinList;

            // kiem tra market da ton tai chua
            basketDic.TryGetValue(ml.MarketID, out currentMarketDic);

            if (currentMarketDic == null)
            {
                // market chua ton tai...
                currentMarketDic = new ConcurrentDictionary<string, List<string>>();
                currentMarketDic.TryAdd(ml.IndexsTypeCode, new List<string> { ml.Symbol });
                basketDic.TryAdd(ml.MarketID, currentMarketDic);
                return true;
            }
            else
            {
                // market da ton tai...
                // kiem tra basket da ton tai chua
                currentMarketDic.TryGetValue(ml.IndexsTypeCode, out isinList);
                if (isinList == null)
                {
                    // basket chua ton tai
					isinList = new List<string>() { ml.Symbol};
                    basketDic[ml.MarketID].TryAdd(ml.IndexsTypeCode, isinList);
                    return true;
                }
                else
                {
                    // basket da ton tai
                    // kiem tra symbol da ton tai trong list chua
                    int index = isinList.FindIndex(s => s.Contains(ml.Symbol));// -1 = not found					
                    if (index == -1)
                    {
                        // chua ton tai thi them vao list
                        basketDic[ml.MarketID][ml.IndexsTypeCode].Add(ml.Symbol);
                        return true;
                    }
                    else
                    {
                        // da ton tai thi bo qua
                        return true;
                    }
                }
            }
        }

        /// <summary>
        /// update LM Dic, luu thong tin VN/EN name cua basket
        /// </summary>
        /// <param name="mlDic"></param>
        /// <param name="indexConstituentsInformation"></param>
        /// <returns></returns>
        public bool UpdateDicML(ref ConcurrentDictionary<string, EXBasket> mlDic, EIndexConstituentsInformation ml)
        {
            if (!mlDic.ContainsKey(ml.IndexsTypeCode))
                mlDic.TryAdd(ml.IndexsTypeCode, new EXBasket() { NameVN = ml.IdxName, NameEN = ml.IdxEnglishName });

            return true;
        }

        /// <summary>
        /// map isin sang tickerCode + sort tang dan
        /// </summary>
        /// <param name="key"></param>
        /// <param name="basketDic"></param>
        /// <param name="symbolDic"></param>
        /// <param name="ml"></param>
        /// <returns></returns>
        public bool UpdateDicBasketMapSort(ref ConcurrentDictionary<string, ConcurrentDictionary<string, List<string>>> basketDic, ref ConcurrentDictionary<string, ESecurityDefinition> symbolDic)
        {
            foreach (KeyValuePair<string, ConcurrentDictionary<string, List<string>>> marketEntry in basketDic) // STO
            {
                foreach (KeyValuePair<string, List<string>> basketEntry in basketDic[marketEntry.Key]) // VN30
                {
                    // map isin sang tickerCode
                    for (int i = 0; i < basketDic[marketEntry.Key][basketEntry.Key].Count; i++)
                        if (symbolDic.ContainsKey(basketDic[marketEntry.Key][basketEntry.Key][i]))
                            basketDic[marketEntry.Key][basketEntry.Key][i] = symbolDic[basketDic[marketEntry.Key][basketEntry.Key][i]].TickerCode;
                    // sort tang dan theo a,b,c
                    //basketDic[marketEntry.Key][basketEntry.Key] = basketDic[marketEntry.Key][basketEntry.Key].OrderBy(q => q).ToList(); // SLOW SPEED, chu y
                }
            }

            return true;
        }

        /// <summary>
        /// 2020-10-06 17:02:32 ngocta2
        /// cap nhat data vao dic Symbol
        /// </summary>
        /// <param name="d"></param>
        /// <returns></returns>
        public bool UpdateDicSymbol(ESecurityDefinition d, ref ConcurrentDictionary<string, ESecurityDefinition> symbolDic)
        {
            // chua co thi insert, da co roi thi update
            symbolDic.AddOrUpdate(d.Symbol, d, (key, val) => val);
            // return
            return true;
        }

        /// <summary>
        /// 2021-10-05 10:42:23 ngocta2
        /// convert MDDS Index (M1) thanh JsonX Index
        /// </summary>
        /// <param name="m1"></param>
        /// <returns></returns>
        public EXIndex Convert_M12XIndex(EIndex m1, ConcurrentDictionary<string, ELastIndex> lastIndexdic)
        {
            // tao new instance
            EXIndex xIndex = new EXIndex(m1.MsgSeqNum);

            //update dic key			
			xIndex.MarketID         = m1.MarketID;
            xIndex.TradingSessionID = m1.TradingSessionID;
			xIndex.TransactTime     = m1.TransactTime;
			xIndex.Index            = m1.IndexsTypeCode;
			xIndex.Value            = m1.ValueIndexes;
			xIndex.TotalQuantity    = m1.TotalVolumeTraded;
			xIndex.TotalValue       = m1.GrossTradeAmt;
			xIndex.NMTotalQuantity  = m1.ContauctAccTrdvol;
			xIndex.NMTotalValue     = m1.ContauctAccTrdval;
			xIndex.PTTotalQuantity  = m1.BlktrdAccTrdvol;
			xIndex.PTTotalValue     = m1.BlktrdAccTrdval;
			xIndex.CeilingCount     = m1.FluctuationUpperLimitIssueCount;
			xIndex.UpCount          = m1.FluctuationUpIssueCount;
			xIndex.NochangeCount    = m1.FluctuationSteadinessIssueCount;
			xIndex.DownCount        = m1.FluctuationDownIssueCount;
			xIndex.FloorCount       = m1.FluctuationLowerLimitIssueCount;
			xIndex.LastValue        = lastIndexdic[m1.IndexsTypeCode].LastIndex;

            // tự tính change + change percent
            if (xIndex.Value > 0 && xIndex.LastValue > 0)
            {
                xIndex.Change = xIndex.Value - xIndex.LastValue;
                xIndex.ChangePercent = (xIndex.Change / xIndex.LastValue) * 100; // -6.756756756756757
                xIndex.ChangePercent = System.Math.Round(Convert.ToDouble(xIndex.ChangePercent), 2); // -6.76
            }
            else
            {
                xIndex.Change = 0;
                xIndex.ChangePercent = 0;
            }

            // return
            return xIndex;
        }

        /// <summary>
        /// 2021-03-24 10:18:15 ngocta2
        /// update dic thong tin gia 
        /// 1 exchange gom nhieu thi truong
        /// 1 market gom nhieu board
        /// 1 board gom nhieu quote
        /// 
        /// newQuote.ISIN = VN2STOST0106
        /// </summary>
        /// <param name="indexDic"></param>
        /// <param name="indexsTypeCode">VN30</param>		
        /// <param name="newIndex">EXIndex obj</param>
        /// <returns></returns>
        public bool UpdateDicIndex(
            ref ConcurrentDictionary<string, EXIndex> indexDic,
            string indexsTypeCode,
            EXIndex newIndex)
        {
            EXIndex currentIndex;

            // kiem tra market da ton tai chua
            indexDic.TryGetValue(indexsTypeCode, out currentIndex);

            if (currentIndex == null)
            {
                // xIndex chua ton tai -> add new
                indexDic.TryAdd(indexsTypeCode, newIndex);
                return true;
            }
            else
            {
                // xIndex da ton tai
                // thi update index theo seqNum
                indexDic[indexsTypeCode] = UpdateXIndexByCellSeqNum(currentIndex, newIndex);
                return true;
            }
        }

        /// <summary>
        //30167=001------30632=VNINDEX
        //30167=101------30632=VN30
        //30167=102------30632=VNMIDCAP
        //30167=103------30632=VNSMALLCAP
        //30167=104------30632=VN100
        //30167=105------30632=VNALLSHARE
        //30167=151------30632=VNXALLSHARE
        //30167=152------30632=VNX50
        //30167=301------30632=VNAllShare Consumer Discretionary
        //30167=302------30632=VNAllShare Consumer Staples
        //30167=303------30632=VNAllShare Energy
        //30167=304------30632=VNAllShare Financials
        //30167=305------30632=VNAllShare Health Care
        //30167=306------30632=VNAllShare Industrials
        //30167=307------30632=VNAllShare Information Technology
        //30167=308------30632=VNAllShare Materials
        //30167=309------30632=VNAllShare Real Estate
        //30167=310------30632=VNAllShare Utilities
        //30167=401------30632=VN Sustainability Index
        //30167=501------30632=VN Diamond Index
        //30167=502------30632=VN Financial Select Sector Index
        //30167=503------30632=VN Leading Financial Index
        //30167=990------30632=PD TEST01
        /// </summary>
        /// <param name="indexDic"></param>
        /// <param name="ml"></param>
        /// <returns></returns>
        public bool UpdateDicIndexCode2Name(ref ConcurrentDictionary<string, EXIndex> indexDic, EIndexConstituentsInformation ml)
        {
            if (indexDic.ContainsKey(ml.IndexsTypeCode))
                indexDic[ml.IndexsTypeCode].Index = ml.IdxName;
            return true;
        }

        /// <summary>
        /// 2021-10-05 14:26:01 ngocta2
        /// so sanh XIndex hien tai va XIndex moi, so sanh theo CellSeqNum
        /// </summary>
        /// <param name="cI">currentIndex</param>
        /// <param name="nI">newIndex</param>
        /// <returns></returns>
        public EXIndex UpdateXIndexByCellSeqNum(EXIndex cI, EXIndex nI)
        {
            string filelog = @"D:\WebLog\S6G\HSXFeederApp\Static\UpdateXIndexByCellSeqNum\" + cI.Index + ".txt";
            CommonLib.Implementations.CCommon.WriteFileStatic(filelog, $"{EGlobalConfig.DateTimeNow} => cQ={Newtonsoft.Json.JsonConvert.SerializeObject(cI)}; nQ={Newtonsoft.Json.JsonConvert.SerializeObject(nI)}");

			if(cI.MarketIDSeqNum        <nI.MarketIDSeqNum        &&nI.MarketID        !=EGlobalConfig.__INIT_NULL_STRING){cI.MarketIDSeqNum        =nI.MarketIDSeqNum        ;cI.MarketID        =nI.MarketID;}
			if(cI.TradingSessionIDSeqNum<nI.TradingSessionIDSeqNum&&nI.TradingSessionID!=EGlobalConfig.__INIT_NULL_STRING){cI.TradingSessionIDSeqNum=nI.TradingSessionIDSeqNum;cI.TradingSessionID=nI.TradingSessionID;}
			if(cI.TransactTimeSeqNum    <nI.TransactTimeSeqNum    &&nI.TransactTime    !=EGlobalConfig.__INIT_NULL_STRING){cI.TransactTimeSeqNum    =nI.TransactTimeSeqNum    ;cI.TransactTime    =nI.TransactTime;}
			if(cI.IndexSeqNum           <nI.IndexSeqNum           &&nI.Index           !=EGlobalConfig.__INIT_NULL_STRING){cI.IndexSeqNum           =nI.IndexSeqNum           ;cI.Index           =nI.Index;}
			if(cI.ValueSeqNum           <nI.ValueSeqNum           &&nI.Value           !=EGlobalConfig.__INIT_NULL_DOUBLE){cI.ValueSeqNum           =nI.ValueSeqNum           ;cI.Value           =nI.Value;}
			if(cI.ChangeSeqNum          <nI.ChangeSeqNum          &&nI.Change          !=EGlobalConfig.__INIT_NULL_DOUBLE){cI.ChangeSeqNum          =nI.ChangeSeqNum          ;cI.Change          =nI.Change;}
			if(cI.ChangePercentSeqNum   <nI.ChangePercentSeqNum   &&nI.ChangePercent   !=EGlobalConfig.__INIT_NULL_DOUBLE){cI.ChangePercentSeqNum   =nI.ChangePercentSeqNum   ;cI.ChangePercent   =nI.ChangePercent;}
			if(cI.TotalQuantitySeqNum   <nI.TotalQuantitySeqNum   &&nI.TotalQuantity   !=EGlobalConfig.__INIT_NULL_LONG  ){cI.TotalQuantitySeqNum   =nI.TotalQuantitySeqNum   ;cI.TotalQuantity   =nI.TotalQuantity;}
			if(cI.TotalValueSeqNum      <nI.TotalValueSeqNum      &&nI.TotalValue      !=EGlobalConfig.__INIT_NULL_DOUBLE){cI.TotalValueSeqNum      =nI.TotalValueSeqNum      ;cI.TotalValue      =nI.TotalValue;}
			if(cI.NMTotalQuantitySeqNum <nI.NMTotalQuantitySeqNum &&nI.NMTotalQuantity !=EGlobalConfig.__INIT_NULL_LONG  ){cI.NMTotalQuantitySeqNum =nI.NMTotalQuantitySeqNum ;cI.NMTotalQuantity =nI.NMTotalQuantity;}
			if(cI.NMTotalValueSeqNum    <nI.NMTotalValueSeqNum    &&nI.NMTotalValue    !=EGlobalConfig.__INIT_NULL_DOUBLE){cI.NMTotalValueSeqNum    =nI.NMTotalValueSeqNum    ;cI.NMTotalValue    =nI.NMTotalValue;}
			if(cI.PTTotalQuantitySeqNum <nI.PTTotalQuantitySeqNum &&nI.PTTotalQuantity !=EGlobalConfig.__INIT_NULL_LONG  ){cI.PTTotalQuantitySeqNum =nI.PTTotalQuantitySeqNum ;cI.PTTotalQuantity =nI.PTTotalQuantity;}
			if(cI.PTTotalValueSeqNum    <nI.PTTotalValueSeqNum    &&nI.PTTotalValue    !=EGlobalConfig.__INIT_NULL_DOUBLE){cI.PTTotalValueSeqNum    =nI.PTTotalValueSeqNum    ;cI.PTTotalValue    =nI.PTTotalValue;}
			if(cI.CeilingCountSeqNum    <nI.CeilingCountSeqNum    &&nI.CeilingCount    !=EGlobalConfig.__INIT_NULL_LONG  ){cI.CeilingCountSeqNum    =nI.CeilingCountSeqNum    ;cI.CeilingCount    =nI.CeilingCount;}
			if(cI.UpCountSeqNum         <nI.UpCountSeqNum         &&nI.UpCount         !=EGlobalConfig.__INIT_NULL_LONG  ){cI.UpCountSeqNum         =nI.UpCountSeqNum         ;cI.UpCount         =nI.UpCount;}
			if(cI.NochangeCountSeqNum   <nI.NochangeCountSeqNum   &&nI.NochangeCount   !=EGlobalConfig.__INIT_NULL_LONG  ){cI.NochangeCountSeqNum   =nI.NochangeCountSeqNum   ;cI.NochangeCount   =nI.NochangeCount;}
			if(cI.DownCountSeqNum       <nI.DownCountSeqNum       &&nI.DownCount       !=EGlobalConfig.__INIT_NULL_LONG  ){cI.DownCountSeqNum       =nI.DownCountSeqNum       ;cI.DownCount       =nI.DownCount;}
			if(cI.FloorCountSeqNum      <nI.FloorCountSeqNum      &&nI.FloorCount      !=EGlobalConfig.__INIT_NULL_LONG  ){cI.FloorCountSeqNum      =nI.FloorCountSeqNum      ;cI.FloorCount      =nI.FloorCount;}
			if(cI.LastValueSeqNum       <nI.LastValueSeqNum       &&nI.LastValue       !=EGlobalConfig.__INIT_NULL_DOUBLE){cI.LastValueSeqNum       =nI.LastValueSeqNum       ;cI.LastValue       =nI.LastValue;}

            return cI;
        }

        /// <summary>
        /// chuyen dic basket thanh list basket
        /// 2021-11-19 16:19:27 ngocta2 fix bo dup ma trong list
        /// "l":"AAV,AAV,ACM,ACM,ADC,ADC,ALT,ALT, 
        /// => 
        /// "l":"AAV,ACM,ADC,ALT,
        /// </summary>
        /// <param name="basketDic"></param>
        /// <returns></returns>
		public List<EXBasket> BasketDic2BasketList(ConcurrentDictionary<string, ConcurrentDictionary<string, List<string>>> basketDic, EIndexConstituentsInformation ml=null, ConcurrentDictionary<string, EXBasket> mlDic=null, List<EXBasket> predefinedBasketList=null)
        {
            List<EXBasket> basketList = new List<EXBasket>();

            foreach (string market in basketDic.Keys) // STO, STX
            {
                foreach (string indexsTypeCode in basketDic[market].Keys) // VN30, VN100
                {
                    var shortName = predefinedBasketList == null ? null : predefinedBasketList.FirstOrDefault(i => i.MarketID == market && i.Name == indexsTypeCode);
                    string nameVN = EGlobalConfig.__STRING_BLANK, nameEN = EGlobalConfig.__STRING_BLANK;
					if (ml != null )
                    {
                        nameVN = ml.IdxName;
                        nameEN = ml.IdxEnglishName;
                    }
                    else
                    {
						if(mlDic != null)
                        {
                            nameVN = mlDic.ContainsKey(indexsTypeCode) ? mlDic[indexsTypeCode].NameVN : EGlobalConfig.__STRING_BLANK;
                            nameEN = mlDic.ContainsKey(indexsTypeCode) ? mlDic[indexsTypeCode].NameEN : EGlobalConfig.__STRING_BLANK;
                        }
                    }


                    EXBasket basket = new EXBasket()
                    {
                        MarketID = market,
						Name     = indexsTypeCode,
						List     = string.Join(EGlobalConfig.__STRING_COMMA, basketDic[market][indexsTypeCode].Distinct().ToList()), // ko cho trung nhau

                        // co data roi nhung sau do update lai ve 0 va "" => bo
                        //Class    = ml != null ? ml.MarketIndexClass : EGlobalConfig.__STRING_BLANK,
						NameVN   = nameVN,
						NameEN   = nameEN,
						Short    = shortName!=null? shortName.Short:indexsTypeCode
                        //Total    = ml != null ? ml.TotalMsgNo : 0
                    };
                    basketList.Add(basket);
                }
            }

            return basketList;
        }

        /// <summary>
        /// chia gia cho 1000 va lam tron so
        /// ProcessPrice(43140,1000,2) = 43.14
        /// ProcessPrice(43140,1000,1) = 43.1
        /// </summary>
        /// <param name="priceString">:43100"</param>
        /// <param name="priceDividedBy">1000</param>
        /// <param name="priceRoundDigitsCount">2</param>
        /// <returns></returns>
        public double ProcessPrice(string priceString, int priceDividedBy, int priceRoundDigitsCount)
        {

            double price = Convert.ToDouble(priceString); // 43100
            price = price / priceDividedBy; // 43.1
            price = Math.Round(price, priceRoundDigitsCount); // 43.1
            return price;
        }
        public int Processkl(string priceString, int priceDividedBy)
        {
            int kl = Convert.ToInt32(priceString); // 43100
            kl = kl / priceDividedBy; // 43.1
            return kl;
        }
    }
}
