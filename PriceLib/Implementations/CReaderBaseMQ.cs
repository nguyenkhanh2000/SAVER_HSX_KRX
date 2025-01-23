
using CommonLib.Implementations;
using CommonLib.Interfaces;
using MSMQ.Messaging;
using Newtonsoft.Json;

//using StockCore.Redis.Entities;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using CommonLib.Implementations;
using StackExchange.Redis;

namespace PriceLib.Implementations
{
    public class CReaderBaseMQ
    {
        protected Object m_objLocker = new Object();
        private readonly IS6GApp _app;
        private readonly CRedisConfig _redisConfig;
        public enum MESSAGE_FORMATTER                                           // xac dinh format XML(IG)  hoac Binary (IS5)
        {
            XML,
            BINARY
        }

        protected MESSAGE_FORMATTER m_enmCurrentMessageFormatter = MESSAGE_FORMATTER.BINARY;
        protected bool m_blnRepMsg;
        protected bool m_blnRepMsg2;
        protected bool m_blnRepMsgPro;
        private long m_lngReadMsgCount = 0;
        protected string m_strSrcQueuePath = "";                                // nguon data chinh, data dau vao cho app
        protected string m_strSrcQueuePath2 = "";                               // REP : nguon data cho => 4.5G => 68
        protected string m_strSrcQueuePathRep = "";                             // REP : nguon data cho => saver5G => redis
        protected string m_strSrcQueuePathPro = "";
        private const string TEMPLATE_REDIS_KEY_REALTIME_DATE = "yyyy:MM:dd"; //(Date)=(yyyy):(MM):(dd)
        public const string FORMAT_TIME_5 = "yyyyMMddHHmmssfff";
        private const string REDIS_KEY_HNX_BI = "S5G_HNX_BI"; //S5G_HNX_BI   
        private Dictionary<string, string> m_dicBI2 = new Dictionary<string, string>();
        public const int intPeriod = 43830; //đủ time cho key sống 1 tháng
        public const string FORMAT_DATETIME_6 = "yyyy-MM-dd HH:mm:ss.fff";
        private const string REGEX_SQL_GET_PROP_VAL = "@(?<SQLParam>.*?)='(?<Val>.*?)'";
        private const string REGEX_SQL_CHECK_IS_SI_EP = "@MsgType='SI'|@MsgType='EP'.*?@ActionType='A'"; // phai la msg SI hoac la msg EP nhung msg EP phai co @ActionType='A' (lenh khop trong phien ATC)
        const string TEMPLATE_JSONC_LE = "{\"MT\":\"(MT)\",\"MQ\":(MQ),\"MP\":(MP),\"TQ\":(TQ)}";    //{"MT":"09:35:12","MQ":12300,"MP":25000,"TQ":11112300}
        private const string REGEX_SQL_GET_SYMBOL = "@Symbol='(?<Symbol>.*?)'";
        private const string REGEX_SQL_REPLACE_REMOVE_TRASH = "@(AdjustQtty|AdjustRate|BeginString|BestBidPrice|BestBidQtty|BestOfferPrice|BestOfferQtty|BodyLength|CouponRate|DateNo|DividentRate|IssueDate|Issuer|MatchValue|MaturityDate|MsgType|Parvalue|PriorClosePrice|PriorOpenPrice|PriorPrice|SellCount|SecurityDesc|SenderCompID|SendingTime|TotalSellTradingQtty|TotalSellTradingValue|TradingUnit|BoardCode|MsgType|Symbol)='.*?'";
        private const string REGEX_SQL_GET_SENDING_TIME = "@SendingTime='(?<SendingTime>.*?)'";
        private const string REGEX_SQL_GET_PROP_VAL_4_LE = "@(?<SQLParam>MatchQtty|MatchPrice|NM_TotalTradedQtty|Time)='(?<Val>.*?)'";
        private const string REGEX_SQL_GET_PROP_VAL_4_PO = "@MsgType='PO'.*?@SendingTime='(?<SendingTime>.*?)',@Symbol='(?<Symbol>.*?)'.*?@BidPrice_1='(?<BidPrice_1>.*?)',@BidQtty_1='(?<BidQtty_1>.*?)',@BidPrice_2='(?<BidPrice_2>.*?)',@BidQtty_2='(?<BidQtty_2>.*?)',@BidPrice_3='(?<BidPrice_3>.*?)',@BidQtty_3='(?<BidQtty_3>.*?)',@OfferPrice_1='(?<OfferPrice_1>.*?)',@OfferQtty_1='(?<OfferQtty_1>.*?)',@OfferPrice_2='(?<OfferPrice_2>.*?)',@OfferQtty_2='(?<OfferQtty_2>.*?)',@OfferPrice_3='(?<OfferPrice_3>.*?)',@OfferQtty_3='(?<OfferQtty_3>.*?)'";
        private Dictionary<string, string> m_dicLE_NM_TotalTradedQtty = new Dictionary<string, string>();
        private const string REGEX_SQL_CHECK_IS_PO = "@MsgType='PO'";
        // data LE (danh sach lenh khop)
        //const string TEMPLATE_JSONC_LE = "{\"MT\":\"(MT)\",\"MQ\":(MQ),\"MP\":(MP),\"TQ\":(TQ)}";    //{"MT":"09:35:12","MQ":12300,"MP":25000,"TQ":11112300}
        const string LE_MT_LONG = "Time";
        const string LE_MT_SHORT = "MT";
        const string LE_MP_LONG = "MatchPrice";
        const string LE_MP_SHORT = "MP";
        const string LE_MQ_LONG = "MatchQtty";
        const string LE_MQ_SHORT = "MQ";
        const string LE_TQ_LONG = "NM_TotalTradedQtty";
        const string LE_TQ_SHORT = "TQ";
        const string TEMPLATE_JSONC_PO = "{\"T\":\"(T)\",\"S\":\"(S)\",\"BP1\":(BP1),\"BQ1\":(BQ1),\"BP2\":(BP2),\"BQ2\":(BQ2),\"BP3\":(BP3),\"BQ3\":(BQ3),\"SP1\":(SP1),\"SQ1\":(SQ1),\"SP2\":(SP2),\"SQ2\":(SQ2),\"SP3\":(SP3),\"SQ3\":(SQ3)}";    //

        private const string REGEX_SQL_GET_TYPE = "@MsgType='(?<Type>.*?)'";
        private const string TEMPLATE_JSONC = "{\"STime\":\"(STime)\",\"SI\":{(SI)},\"TP\":{(TP)}}"; // {"SI":{}, "TP":{"ST":"20151119-09:00:21","NTP":"2","BBP1":"19900","BBQ1":"3000","BBP2":"18500","BBQ2":"1000"}}
        private const string TYPE_SI = "SI";
        private const string TYPE_TP = "TP";

        public const string MSG_TYPE_INDEX = "I";      // HNX30/HNX30TRI/HNXCon/HNXFin/HNXIndex/HNXLCap/HNXMan/HNXMSCap/HNXUpcomIndex
        public const string MSG_TYPE_BASKET_INDEX = "S";      // HNX30/HNXCon/HNXFin/HNXIndex/HNXLCap/HNXMan/HNXMSCap/HNXUpcomIndex
        public const string MSG_TYPE_STOCK_INFO = "SI";     // HA+UP    => ListCodeABC
        public const string MSG_TYPE_TOP_N_PRICE = "TP";     // HA+UP    => ListCodeABC
        public const string MSG_TYPE_BOARD_INFO = "BI";     // HA+UP
        public const string MSG_TYPE_AUTION_MATCH = "EP";     // HA       => gia du kien phien ATC
        public const string MSG_TYPE_TOP_PRICE_ODDLOT = "PO";     // HA+UP    => ListCodeABC
        public const string MSG_TYPE_ETF_NET_VALUE = "IV";     // E1SSHN30
        public const string MSG_TYPE_ETF_TRACKING_ERROR = "TE";     // no data
        public const string MSG_TYPE_DERIVATIVES_INFO = "DI";     // thị trường Phái sinh. DerivativesQuotes
        public const string MSG_TYPE_DERIVATIVES_INFO_ORACLE = "DIO";     //2018-07-20 15:38:58 hungpv
        public const string MSG_TYPE_DERIVATIVES_INFO_A_LOGIN = "A";     //2018-11-01 09:15:46 hungpv
        public const string MSG_TYPE_DERIVATIVES_INFO_TP = "DI_TP";  // thị trường Phái sinh. DerivativesQuotes, TP nhung chua data cua thi truong phai sinh: unit va index deu khac voi TP cua thi truong co so

        public const string MSG_TYPE_STOCK_INFO_PT = "SI_PT";  // HA+UP, msg nay ko co that, chi dung de xu ly du lieu PT, dang lay ra tu msg SI 2015-12-16 10:32:39 ngocta2 (SI_PT) >> UPCOM cung co giao dich PT

        private Dictionary<int, string> m_dicI = new Dictionary<int, string>();
        private Dictionary<int, string> m_dicS = new Dictionary<int, string>();
        private Dictionary<int, string> m_dicSI = new Dictionary<int, string>();
        private Dictionary<int, string> m_dicTP = new Dictionary<int, string>();
        public Dictionary<int, string> m_dicBI = new Dictionary<int, string>();
        private Dictionary<int, string> m_dicEP = new Dictionary<int, string>();
        private Dictionary<int, string> m_dicPO = new Dictionary<int, string>();
        private Dictionary<int, string> m_dicIV = new Dictionary<int, string>();
        private Dictionary<int, string> m_dicTE = new Dictionary<int, string>();
        private Dictionary<int, string> m_dicDI = new Dictionary<int, string>();
        private Dictionary<int, string> m_dicDIO = new Dictionary<int, string>();
        private Dictionary<int, string> m_dicLogon = new Dictionary<int, string>();
        public enum REPLACE_TAGS                                                // xac dinh muc dich replace tag, khi pub can tiet kiem bandwith, de ten fieldName ngan nhat co the
        {
            FOR_PUB,                                                            // de pub vao channel : 425 => f425
            FOR_DB                                                              // de exec sp insert db : 425 => BoardCode
        }
        private const string REGEX_SQL_CHECK_IS_SI_TP = "@MsgType='TP|SI'";
        private const string TEMPLATE_REDIS_KEY_REALTIME = "REALTIME:S5G_(Symbol)"; //   REALTIME:S5G_(Symbol)
        private const string TEMPLATE_REDIS_KEY_LE = "LE:S5G_(Symbol)"; //   LE:S5G_(Symbol)
        private const string TEMPLATE_REDIS_KEY_PO = "PO:S5G_(Symbol)"; //   PO:S5G_(Symbol)
        public const string TEMPLATE_SP_S5G_HNX_SAVER = "prc_S5G_HNX_SAVER_IG_(Type)_UPDATE"; //prc_S5G_HNX_SAVER_IG_SI_UPDATE
        public bool RepMessage    // true/false
        {
            get { return this.m_blnRepMsg; }
            set { this.m_blnRepMsg = value; }
        }
        public bool RepMessage2    // true/false
        {
            get { return this.m_blnRepMsg2; }
            set { this.m_blnRepMsg2 = value; }
        }
        public bool RepMessagePro    // true/false
        {
            get { return this.m_blnRepMsgPro; }
            set { this.m_blnRepMsgPro = value; }
        }
        public long ReadMsgCount// tong so msg da read, se tang dan
        {
            get { return this.m_lngReadMsgCount; }
        }
        public string SrcQueuePath // main source data : msg queue cho this app
        {
            get { return this.m_strSrcQueuePath; }
            set { this.m_strSrcQueuePath = value; }
        }

        public string SrcQueuePath2 // REP : nguon data cho => 4.5G => 68
        {
            get { return this.m_strSrcQueuePath2; }
            set { this.m_strSrcQueuePath2 = value; }
        }

        public string SrcQueuePathRep // REP : nguon data cho => saver5G => redis
        {
            get { return this.m_strSrcQueuePathRep; }
            set { this.m_strSrcQueuePathRep = value; }
        }

        public string SrcQueuePathPro // REP : nguon data cho => feederPro5G => IIS
        {
            get { return this.m_strSrcQueuePathPro; }
            set { this.m_strSrcQueuePathPro = value; }
        }
        public CReaderBaseMQ(IS6GApp app, CRedisConfig redisConfig)
        {
            this._app = app;
            this._redisConfig = redisConfig;
            m_strSrcQueuePath = this._redisConfig.Src_QueueWindown;
            this.m_dicI = this.InitDic(MSG_TYPE_INDEX);                  // MSG_TYPE_MESSAGE_INDEX = "I";
            this.m_dicS = this.InitDic(MSG_TYPE_BASKET_INDEX);           // MSG_TYPE_BASKET_INDEX = "S";
            this.m_dicSI = this.InitDic(MSG_TYPE_STOCK_INFO);             // MSG_TYPE_STOCK_INFO = "SI";
            this.m_dicTP = this.InitDic(MSG_TYPE_TOP_N_PRICE);            // MSG_TYPE_TOP_N_PRICE = "TP";
            this.m_dicBI = this.InitDic(MSG_TYPE_BOARD_INFO);             // MSG_TYPE_BOARD_INFO = "BI";
            this.m_dicEP = this.InitDic(MSG_TYPE_AUTION_MATCH);           // MSG_TYPE_AUTION_MATCH = "EP";
            this.m_dicPO = this.InitDic(MSG_TYPE_TOP_PRICE_ODDLOT);       // MSG_TYPE_TOP_PRICE_ODDLOT = "PO";
            this.m_dicIV = this.InitDic(MSG_TYPE_ETF_NET_VALUE);          // MSG_TYPE_ETF_NET_VALUE = "IV";
            this.m_dicTE = this.InitDic(MSG_TYPE_ETF_TRACKING_ERROR);     // MSG_TYPE_ETF_TRACKING_ERROR = "TE";
            this.m_dicDI = this.InitDic(MSG_TYPE_DERIVATIVES_INFO);       // MSG_TYPE_DERIVATIVES_INFO = "DI";
            this.m_dicDIO = this.InitDic(MSG_TYPE_DERIVATIVES_INFO_ORACLE);       // MSG_TYPE_DERIVATIVES_INFO_ORACLE = "DIO";
            this.m_dicLogon = this.InitDic(MSG_TYPE_DERIVATIVES_INFO_A_LOGIN);       // MSG_TYPE_DERIVATIVES_INFO_ORACLE = "A";
        }
        public Dictionary<int, string> InitDic(string strType)
        {
            try
            {
                Dictionary<int, string> dic = new Dictionary<int, string>();                     // dic tuy thuoc vao tung type

                switch (strType)
                {
                    case MSG_TYPE_INDEX:      // "I";
                        //'1.'Private Const MSG_TYPE_MESSAGE_INDEX$ = "35=I"                  
                        dic.Add(8, "BeginString");
                        dic.Add(9, "BodyLength");
                        dic.Add(35, "MsgType");
                        dic.Add(49, "SenderCompID");
                        dic.Add(52, "SendingTime");
                        //'------------------------------
                        dic.Add(1, "IDIndex");
                        dic.Add(2, "IndexCode");
                        dic.Add(3, "Value");
                        dic.Add(4, "CalTime");
                        dic.Add(5, "Change");
                        dic.Add(6, "RatioChange");
                        dic.Add(7, "TotalQtty");
                        dic.Add(14, "TotalValue");
                        dic.Add(19, "TradingDate");
                        dic.Add(21, "CurrentStatus");
                        dic.Add(22, "TotalStock");
                        dic.Add(23, "PriorIndexVal");
                        dic.Add(24, "HighestIndex");
                        dic.Add(25, "LowestIndex");
                        dic.Add(26, "CloseIndex");
                        dic.Add(27, "TypeIndex");
                        dic.Add(18, "IndexName");
                        break;
                    case MSG_TYPE_BASKET_INDEX:       // "S";
                        //'2.'Private Const MSG_TYPE_BASKET_INDEX$ = "35=S"                   
                        dic.Add(8, "BeginString");
                        dic.Add(9, "BodyLength");
                        dic.Add(35, "MsgType");
                        dic.Add(49, "SenderCompID");
                        dic.Add(52, "SendingTime");
                        //'------------------------------
                        dic.Add(1, "IDIndex");
                        dic.Add(2, "IndexCode");
                        dic.Add(15, "IDSymbol");
                        dic.Add(55, "Symbol");
                        dic.Add(11, "TotalQtty");
                        dic.Add(12, "Weighted");
                        dic.Add(28, "AddDate");
                        break;
                    case MSG_TYPE_STOCK_INFO:         // "SI";
                        //'3.'Private Const MSG_TYPE_STOCK_INFO$ = "35=SI"                    
                        dic.Add(8, "BeginString");
                        dic.Add(9, "BodyLength");
                        dic.Add(35, "MsgType");
                        dic.Add(49, "SenderCompID");
                        dic.Add(52, "SendingTime");
                        //'------------------------------
                        dic.Add(55, "Symbol");
                        dic.Add(15, "IDSymbol");
                        dic.Add(425, "BoardCode");
                        dic.Add(336, "TradingSessionID");
                        dic.Add(340, "TradSesStatus");
                        dic.Add(326, "SecurityTradingStatus");
                        dic.Add(327, "ListingStatus");
                        dic.Add(167, "SecurityType");
                        dic.Add(225, "IssueDate");
                        dic.Add(106, "Issuer");
                        dic.Add(107, "SecurityDesc");
                        dic.Add(132, "BestBidPrice");
                        dic.Add(1321, "BestBidQtty");
                        dic.Add(133, "BestOfferPrice");
                        dic.Add(1331, "BestOfferQtty");
                        dic.Add(134, "TotalBidQtty");
                        dic.Add(135, "TotalOfferQtty");
                        dic.Add(260, "BasicPrice");
                        dic.Add(333, "FloorPrice");
                        dic.Add(332, "CeilingPrice");
                        dic.Add(334, "Parvalue");
                        dic.Add(31, "MatchPrice");
                        dic.Add(32, "MatchQtty");
                        dic.Add(137, "OpenPrice");
                        dic.Add(138, "PriorOpenPrice");
                        dic.Add(139, "ClosePrice");
                        dic.Add(140, "PriorClosePrice");
                        dic.Add(387, "TotalVolumeTraded");
                        dic.Add(3871, "TotalValueTraded");
                        dic.Add(631, "MidPx");
                        dic.Add(388, "Tradingdate");
                        dic.Add(399, "Time");
                        dic.Add(400, "TradingUnit");
                        dic.Add(109, "TotalListingQtty");
                        dic.Add(17, "DateNo");
                        dic.Add(230, "AdjustQtty");
                        dic.Add(232, "ReferenceStatus");
                        dic.Add(233, "AdjustRate");
                        dic.Add(244, "DividentRate");
                        dic.Add(255, "CurrentPrice");
                        dic.Add(2551, "CurrentQtty");
                        dic.Add(266, "HighestPice");
                        dic.Add(2661, "LowestPrice");
                        dic.Add(277, "PriorPrice");
                        dic.Add(310, "MatchValue");
                        dic.Add(320, "OfferCount");
                        dic.Add(321, "BidCount");
                        dic.Add(391, "NM_TotalTradedQtty");
                        dic.Add(392, "NM_TotalTradedValue");
                        dic.Add(393, "PT_MatchQtty");
                        dic.Add(3931, "PT_MatchPrice");
                        dic.Add(394, "PT_TotalTradedQtty");
                        dic.Add(3941, "PT_TotalTradedValue");
                        dic.Add(395, "TotalBuyTradingQtty");
                        dic.Add(3951, "BuyCount");
                        dic.Add(3952, "TotalBuyTradingValue");
                        dic.Add(396, "TotalSellTradingQtty");
                        dic.Add(3961, "SellCount");
                        dic.Add(3962, "TotalSellTradingValue");
                        dic.Add(397, "BuyForeignQtty");
                        dic.Add(3971, "BuyForeignValue");
                        dic.Add(398, "SellForeignQtty");
                        dic.Add(3981, "SellForeignValue");
                        dic.Add(3301, "RemainForeignQtty");
                        dic.Add(541, "MaturityDate");
                        dic.Add(223, "CouponRate");
                        dic.Add(1341, "TotalBidQtty_OD");
                        dic.Add(1351, "TotalOfferQtty_OD");
                        dic.Add(13, "fx13");        // MatchChange
                        dic.Add(311, "fx311");        // gia khop gan nhat
                                                     
                        break;
                    case MSG_TYPE_TOP_N_PRICE:        // "TP";
                        //'4.'Private Const MSG_TYPE_TOP_N_PRICE$ = "35=TP"                   
                        dic.Add(8, "BeginString");
                        dic.Add(9, "BodyLength");
                        dic.Add(35, "MsgType");
                        dic.Add(49, "SenderCompID");
                        dic.Add(52, "SendingTime");
                        //'------------------------------
                        dic.Add(55, "Symbol");
                        dic.Add(425, "BoardCode");
                        dic.Add(555, "NoTopPrice");
                        dic.Add(556, "NumTopPrice");
                        dic.Add(132, "BestBidPrice");
                        dic.Add(1321, "BestBidQtty");
                        dic.Add(133, "BestOfferPrice");
                        dic.Add(1331, "BestOfferQtty");
                        break;
                    case MSG_TYPE_BOARD_INFO:         // "BI";
                        //'5.'Private Const MSG_TYPE_BOARD_INFO$ = "35=BI"            
                        dic.Add(8, "BeginString");
                        dic.Add(9, "BodyLength");
                        dic.Add(35, "MsgType");
                        dic.Add(49, "SenderCompID");
                        dic.Add(52, "SendingTime");
                        //'------------------------------
                        dic.Add(425, "BoardCode");
                        dic.Add(426, "BoardStatus");
                        dic.Add(336, "TradingSessionID");
                        dic.Add(340, "TradSesStatus");
                        dic.Add(421, "Name");
                        dic.Add(422, "Shortname");
                        dic.Add(388, "Tradingdate");
                        dic.Add(399, "Time");
                        dic.Add(270, "TotalTrade");
                        dic.Add(250, "TotalStock");
                        dic.Add(251, "numSymbolAdvances");
                        dic.Add(252, "numSymbolNochange");
                        dic.Add(253, "numSymbolDeclines");
                        dic.Add(17, "DateNo");
                        dic.Add(220, "totalNormalTradedQttyRd");
                        dic.Add(221, "totalNormalTradedValueRd");
                        dic.Add(210, "totalNormalTradedQttyOd");
                        dic.Add(211, "totalNormalTradedValueOd");
                        dic.Add(240, "totalPTTradedQtty");
                        dic.Add(241, "totalPTTradedValue");
                        dic.Add(341, "f341");
                        break;
                    case MSG_TYPE_AUTION_MATCH:       // "EP";
                        //'6.'Private Const MSG_TYPE_AUTION_MATCH$ = "35=EP"          
                        dic.Add(8, "BeginString");
                        dic.Add(9, "BodyLength");
                        dic.Add(35, "MsgType");
                        dic.Add(49, "SenderCompID");
                        dic.Add(52, "SendingTime");
                        //'------------------------------
                        dic.Add(33, "ActionType");
                        dic.Add(55, "Symbol");
                        dic.Add(31, "Price");
                        dic.Add(32, "Qtty");
                        dic.Add(336, "TradingSessionID");
                        break;
                    case MSG_TYPE_TOP_PRICE_ODDLOT:   // "PO";
                        //'7.'Private Const MSG_TYPE_TOP_PRICE_ODDLOT$ = "35=PO"
                        dic.Add(8, "BeginString");
                        dic.Add(9, "BodyLength");
                        dic.Add(35, "MsgType");
                        dic.Add(49, "SenderCompID");
                        dic.Add(52, "SendingTime");
                        //''------------------------------
                        dic.Add(55, "Symbol");
                        dic.Add(425, "BoardCode");
                        dic.Add(132, "BidPrice_1");
                        dic.Add(1321, "BidQtty_1");
                        dic.Add(133, "BidPrice_2");
                        dic.Add(1331, "BidQtty_2");
                        dic.Add(134, "BidPrice_3");
                        dic.Add(1341, "BidQtty_3");
                        dic.Add(135, "OfferPrice_1");
                        dic.Add(1351, "OfferQtty_1");
                        dic.Add(136, "OfferPrice_2");
                        dic.Add(1361, "OfferQtty_2");
                        dic.Add(137, "OfferPrice_3");
                        dic.Add(1371, "OfferQtty_3");
                        break;
                    case MSG_TYPE_ETF_NET_VALUE:      // "IV";
                        //'8.'Private Const MSG_TYPE_ETF_NET_VALUE$ = "35=IV"         '8.
                        dic.Add(8, "BeginString");
                        dic.Add(9, "BodyLength");
                        dic.Add(35, "MsgType");
                        dic.Add(49, "SenderCompID");
                        dic.Add(52, "SendingTime");
                        //'------------------------------
                        dic.Add(56, "CODE");
                        dic.Add(57, "TIME");
                        dic.Add(58, "INAV");
                        break;
                    case MSG_TYPE_ETF_TRACKING_ERROR: // "TE";
                        //'9.'Private Const MSG_TYPE_ETF_TRACKING_ERROR$ = "35=TE"    '9.
                        dic.Add(8, "BeginString");
                        dic.Add(9, "BodyLength");
                        dic.Add(35, "MsgType");
                        dic.Add(49, "SenderCompID");
                        dic.Add(52, "SendingTime");
                        //'------------------------------
                        dic.Add(56, "CODE");
                        dic.Add(59, "WEEK");
                        dic.Add(60, "DATE");
                        dic.Add(61, "TE_VALUE");
                        break;
                    case MSG_TYPE_DERIVATIVES_INFO:         // "DI";
                        //MSG_TYPE_DERIVATIVES_INFO = "35=DI"
                        dic.Add(8, "BeginString");
                        dic.Add(9, "BodyLength");
                        dic.Add(35, "MsgType");
                        dic.Add(49, "SenderCompID");
                        dic.Add(52, "SendingTime");
                        //'------------------------------
                        dic.Add(55, "Symbol");                    //2018-05-30 09:31:55 ngocta2
                        dic.Add(15, "SymbolID");
                        dic.Add(800, "Underlying");
                        dic.Add(425, "BoardCode");
                        dic.Add(336, "TradingSessionID");
                        dic.Add(340, "TradeSesStatus");
                        dic.Add(326, "SecurityTradingStatus");
                        dic.Add(327, "ListingStatus");
                        dic.Add(167, "SecurityType");
                        dic.Add(801, "OpenInterest");
                        dic.Add(8011, "OpenInterestChange");
                        dic.Add(802, "FirstTradingDate");
                        dic.Add(803, "LastTradingDate");
                        dic.Add(132, "BestBidPrice");
                        dic.Add(1321, "BestBidQtty");
                        dic.Add(133, "BestOfferPrice");
                        dic.Add(1331, "BestOfferQtty");
                        dic.Add(134, "TotalBidQtty");
                        dic.Add(135, "TotalOfferQtty");
                        dic.Add(260, "BasicPrice");
                        dic.Add(333, "FloorPrice");
                        dic.Add(332, "CeilingPrice");
                        dic.Add(31, "MatchPrice");
                        dic.Add(32, "MatchQtty");
                        dic.Add(137, "OpenPrice");
                        dic.Add(138, "PriorOpenPrice");
                        dic.Add(804, "OpenQtty");
                        dic.Add(139, "ClosePrice");
                        dic.Add(140, "PriorClosePrice");
                        dic.Add(805, "CloseQtty");
                        dic.Add(387, "TotalVolumeTraded");
                        dic.Add(3871, "TotalValueTraded");
                        dic.Add(388, "Tradingdate");
                        dic.Add(399, "Time");
                        dic.Add(400, "TradingUnit");
                        dic.Add(17, "DateNo");
                        dic.Add(255, "CurrentPrice");
                        dic.Add(2551, "CurrentQtty");
                        dic.Add(266, "HighestPrice");
                        dic.Add(2661, "LowestPrice");
                        dic.Add(310, "MatchValue");
                        dic.Add(320, "OfferCount");
                        dic.Add(321, "BidCount");
                        dic.Add(391, "NM_TotalTradedQtty");
                        dic.Add(392, "NM_TotalTradedValue");
                        dic.Add(393, "PT_MatchQtty");
                        dic.Add(3931, "PT_MatchPrice");
                        dic.Add(394, "PT_TotalTradedQtty");
                        dic.Add(3941, "PT_TotalTradedValue");
                        dic.Add(814, "NM_BuyForeignQtty");
                        dic.Add(815, "PT_BuyForeignQtty");
                        dic.Add(397, "BuyForeignQtty");
                        dic.Add(8141, "NM_BuyForeignValue");
                        dic.Add(8151, "PT_BuyForeignValue");
                        dic.Add(3971, "BuyForeignValue");
                        dic.Add(816, "NM_SellForeignQtty");
                        dic.Add(817, "PT_SellForeignQtty");
                        dic.Add(398, "SellForeignQtty");
                        dic.Add(8161, "NM_SellForeignValue");
                        dic.Add(8171, "PT_SellForeignValue");
                        dic.Add(3981, "SellForeignValue");
                        break;

                    case MSG_TYPE_DERIVATIVES_INFO_ORACLE:         // "DI";2018-07-20 12:26:56 hungpv

                        dic.Add(8, "-1oracle=BeginString");
                        dic.Add(9, "-1oracle=BodyLength");
                        dic.Add(35, "-1oracle=MsgType");
                        dic.Add(49, "-1oracle=SenderCompID");
                        dic.Add(52, "-1oracle=SendingTime");
                        //'------------------------------
                        dic.Add(55, "0oracle=p_Symbol");
                        dic.Add(15, "1oracle=p_SymbolID");
                        dic.Add(800, "2oracle=p_Underlying");
                        dic.Add(425, "3oracle=p_BoardCode");
                        dic.Add(336, "4oracle=p_TradingSessionID");
                        dic.Add(340, "5oracle=p_TradeSesStatus");
                        dic.Add(326, "6oracle=p_SecurityTradingStatus");
                        dic.Add(327, "7oracle=p_ListingStatus");
                        dic.Add(167, "8oracle=p_SecurityType");
                        dic.Add(801, "9oracle=p_OpenInterest");
                        dic.Add(8011, "10oracle=p_OpenInterestChange");
                        dic.Add(802, "11oracle=p_FirstTradingDate");
                        dic.Add(803, "12oracle=p_LastTradingDate");
                        dic.Add(132, "13oracle=p_BestBidPrice");
                        dic.Add(1321, "14oracle=p_BestBidQtty");
                        dic.Add(133, "15oracle=p_BestOfferPrice");
                        dic.Add(1331, "16oracle=p_BestOfferQtty");
                        dic.Add(134, "17oracle=p_TotalBidQtty");
                        dic.Add(135, "18oracle=p_TotalOfferQtty");
                        dic.Add(260, "19oracle=p_BasicPrice");
                        dic.Add(333, "20oracle=p_FloorPrice");
                        dic.Add(332, "21oracle=p_CeilingPrice");
                        dic.Add(31, "22oracle=p_MatchPrice");
                        dic.Add(32, "23oracle=p_MatchQtty");
                        dic.Add(137, "24oracle=p_OpenPrice");
                        dic.Add(138, "25oracle=p_PriorOpenPrice");
                        dic.Add(804, "26oracle=p_OpenQtty");
                        dic.Add(139, "27oracle=p_ClosePrice");
                        dic.Add(140, "28oracle=p_PriorClosePrice");
                        dic.Add(805, "29oracle=p_CloseQtty");
                        dic.Add(387, "30oracle=p_TotalVolumeTraded");
                        dic.Add(3871, "31oracle=p_TotalValueTraded");
                        dic.Add(388, "32oracle=p_Tradingdate");
                        dic.Add(399, "33oracle=p_Time");
                        dic.Add(400, "34oracle=p_TradingUnit");
                        dic.Add(17, "35oracle=p_DateNo");
                        dic.Add(255, "36oracle=p_CurrentPrice");
                        dic.Add(2551, "37oracle=p_CurrentQtty");
                        dic.Add(266, "38=oraclep_HighestPrice");
                        dic.Add(2661, "39oracle=p_LowestPrice");
                        dic.Add(310, "40oracle=p_MatchValue");
                        dic.Add(320, "41oracle=p_OfferCount");
                        dic.Add(321, "42oracle=p_BidCount");
                        dic.Add(391, "43oracle=p_NM_TotalTradedQtty");
                        dic.Add(392, "44oracle=p_NM_TotalTradedValue");
                        dic.Add(393, "45oracle=p_PT_MatchQtty");
                        dic.Add(3931, "46oracle=p_PT_MatchPrice");
                        dic.Add(394, "47oracle=p_PT_TotalTradedQtty");
                        dic.Add(3941, "48oracle=p_PT_TotalTradedValue");
                        dic.Add(814, "49oracle=p_NM_BuyForeignQtty");
                        dic.Add(815, "50oracle=p_PT_BuyForeignQtty");
                        dic.Add(397, "51oracle=p_BuyForeignQtty");
                        dic.Add(8141, "52oracle=p_NM_BuyForeignValue");
                        dic.Add(8151, "53oracle=p_PT_BuyForeignValue");
                        dic.Add(3971, "54oracle=p_BuyForeignValue");
                        dic.Add(816, "55oracle=p_NM_SellForeignQtty");
                        dic.Add(817, "56oracle=p_PT_SellForeignQtty");
                        dic.Add(398, "57oracle=p_SellForeignQtty");
                        dic.Add(8161, "58oracle=p_NM_SellForeignValue");
                        dic.Add(8171, "59oracle=p_PT_SellForeignValue");
                        dic.Add(3981, "60oracle=p_SellForeignValue");
                        break;
                    case MSG_TYPE_DERIVATIVES_INFO_A_LOGIN:      // "I";
                        //'1.'Private Const MSG_TYPE_MESSAGE_INDEX$ = "35=I"                  
                        dic.Add(8, "BeginString");
                        dic.Add(9, "BodyLength");
                        dic.Add(35, "MsgType");
                        dic.Add(49, "SenderCompID");
                        dic.Add(52, "SendingTime");
                        //'------------------------------
                        dic.Add(58, "Logon_Infor");
                        break;


                }

                return dic;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return null;
            }

        }
        public string Fix2Json(string fixString)
        {
            StringBuilder sb = new StringBuilder(fixString);
            sb.Length--;
            sb.Replace("", "\",\"");
            sb.Replace("=", "\":\"");
            sb.Append("\"}");
            sb.Insert(0, "{\"");
            return sb.ToString();
        }
      
        protected bool Dic_AddOrUpdate(ref Dictionary<string, string> dic, string strKey, string strValue)
        {
            try
            {
                string strOut = "";

                // xu ly dic
                if (dic.TryGetValue(strKey, out strOut))
                    dic[strKey] = strValue; // update existing key
                else
                    dic.Add(strKey, strValue);// add new key

                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }
        public bool ReplaceTags(string strMessage, ref string strType, ref string strResult, ref string strResultOracle, ref string[] arrPart, REPLACE_TAGS RT)
        {
            try
            {
                Dictionary<int, string> dic = null;                     // dic tuy thuoc vao tung type
                Dictionary<int, string> dicOracle = null;                     // dic tuy thuoc vao tung type
                //string[] arrPart = strMessage.Split((char)1);           // cac thong tin dc cach nhau bang ky tu co ma ascii=1 (ko nhin thay dc trong 1 so editor nhu NotePad, phai dung NotePad++ moi thay duoc)                
                arrPart = strMessage.Split((char)1);           // cac thong tin dc cach nhau bang ky tu co ma ascii=1 (ko nhin thay dc trong 1 so editor nhu NotePad, phai dung NotePad++ moi thay duoc)                
                StringBuilder sb = new StringBuilder(strMessage, strMessage.Length * 2); // khai bao string builder va phan chia mem
                string strSeparator = ((char)1).ToString();
                StringBuilder sbOracle = new StringBuilder(strMessage, strMessage.Length * 2);
                // xac dinh type
                strType = arrPart[2].Substring(3);       // "35=BI"=> "BI"

                
                switch (strType)
                {
                    case MSG_TYPE_INDEX:      // "I";
                        dic = this.m_dicI;
                        break;
                    case MSG_TYPE_BASKET_INDEX:       // "S";
                        dic = this.m_dicS;
                        break;
                    case MSG_TYPE_STOCK_INFO:         // "SI";
                        dic = this.m_dicSI;
                        break;
                    case MSG_TYPE_TOP_N_PRICE:        // "TP";
                        dic = this.m_dicTP;
                        break;
                    case MSG_TYPE_BOARD_INFO:         // "BI";
                        dic = this.m_dicBI;
                        break;
                    case MSG_TYPE_AUTION_MATCH:       // "EP";
                        dic = this.m_dicEP;
                        break;
                    case MSG_TYPE_TOP_PRICE_ODDLOT:   // "PO";
                        dic = this.m_dicPO;
                        break;
                    case MSG_TYPE_ETF_NET_VALUE:      // "IV";
                        dic = this.m_dicIV;
                        break;
                    case MSG_TYPE_ETF_TRACKING_ERROR: // "TE";
                        dic = this.m_dicTE;
                        break;
                    case MSG_TYPE_DERIVATIVES_INFO:   // "DI";
                        dic = this.m_dicDI;
                        dicOracle = this.m_dicDIO;
                        break;
                    case MSG_TYPE_DERIVATIVES_INFO_A_LOGIN:
                        dic = this.m_dicLogon;
                        break;


                      default:
                        strResult = "";
                        return false;
                }

                // them strSeparator vao dau tien
                sb.Insert(0, strSeparator);

                // replace moi ky tu so thanh keyword
                foreach (int k in dic.Keys)
                {
                    if (RT == REPLACE_TAGS.FOR_DB)
                        sb.Replace(strSeparator + k.ToString() + "=", strSeparator + dic[k] + "=");
                    if (RT == REPLACE_TAGS.FOR_PUB)
                        sb.Replace(strSeparator + k.ToString() + "=", strSeparator + "f" + k.ToString() + "=");
                }

                if (dicOracle != null)
                {
                    sbOracle.Length -= 1;
                    sbOracle.Insert(0, strSeparator);
                    foreach (var k in dicOracle.Keys)
                    {
                        sbOracle.Replace(strSeparator + k.ToString() + "=", strSeparator + dicOracle[k] + "=");
                        if (sbOracle.ToString().IndexOf(dicOracle[k]) <= -1)
                        {
                            sbOracle.Append(strSeparator + dicOracle[k] + "=");
                        }
                    }
                    // xoa strSeparator cho dau tien
                    sbOracle.Remove(0, 1);

                    sbOracle.Replace("oracle", "");
                    //2018-07-20 15:43:42 hungpv
                    strResultOracle = sbOracle.ToString();
                }


                // xoa strSeparator cho dau tien
                sb.Remove(0, 1);

                // output ref string
                strResult = sb.ToString();

                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }
        private string SQL2JsonC_LE(string strSQL, ref string strSymbol)
        {
            try
            {
                //                const string DEBUG_CODE = "ADC";

                // ko phai SI thi exit
                //EXEC prc_S5G_HNX_SAVER_IG_SI_UPDATE @BeginString='HNX.TDS.1',@BodyLength='0633',@MsgType='SI',@SenderCompID='HNX',@SendingTime='20160610-15:02:44',@IDSymbol='4864',@Symbol='KKC',@SecurityType='ST',@IssueDate='00010101-12:01:00',@CeilingPrice='15400',@FloorPrice='12600',@SecurityTradingStatus='0',@BasicPrice='14000',@BestBidPrice='14200',@BestBidQtty='500',@BestOfferQtty='1200',@BestOfferPrice='14500',@TotalBidQtty='30200.000000',@TotalOfferQtty='17900.000000',@ClosePrice='14500',@MatchQtty='100',@MatchPrice='14500',@TotalVolumeTraded='13200.000000',@TotalValueTraded='185960000.000000',@BidCount='48',@NM_TotalTradedValue='185960000.000000',@BoardCode='LIS_BRD_01',@TotalBuyTradingValue='185960000.000000',@TotalBuyTradingQtty='13200.000000',@TotalSellTradingValue='185960000.000000',@TotalSellTradingQtty='13200.000000',@RemainForeignQtty='2081400',@BuyCount='35',@SellCount='35',@CurrentPrice='14500',@CurrentQtty='200',@Parvalue='10000',@OpenPrice='13800',@PriorOpenPrice='13800',@PriorClosePrice='14500',@MidPx='14082',@Tradingdate='20160610',@Time='15:02:44',@TradingUnit='100',@TotalListingQtty='5200000.000000',@DateNo='1944',@MatchValue='1450000.000000',@HighestPice='14500',@LowestPrice='13800',@NM_TotalTradedQtty='13200.000000',@ReferenceStatus='0',@TradingSessionID='LIS_PTH_P_NML',@TradSesStatus='97',@OfferCount='21',@ListingStatus='2'
                //EXEC prc_S5G_HNX_SAVER_IG_EP_UPDATE @BeginString='HNX.TDS.1',@BodyLength='0062',@MsgType='EP',@SenderCompID='HNX',@SendingTime='20160610-14:45:09',@Price='14500',@Qtty='200',@Symbol='KKC',@ActionType='A'
                if (!Regex.IsMatch(strSQL, REGEX_SQL_CHECK_IS_SI_EP))
                    return "";

                //----------------------------------------------------------

                // declare
                StringBuilder sbJsonC = new StringBuilder(TEMPLATE_JSONC_LE);
                string strJsonC = "";
                int intMatchCount = 0;

                //if (strSymbol == DEBUG_CODE) CLog.LogEx("LE.txt", "strSQL=" + strSQL);

                // lay symbol tu strSQL (SI/TP)
                strSymbol = Regex.Match(strSQL, REGEX_SQL_GET_SYMBOL).Groups[1].Value;

                // lay ra tat ca Prop/Val trong strReplacedSQL
                Regex RegexObj = new Regex(REGEX_SQL_GET_PROP_VAL_4_LE);
                Match MatchResults = RegexObj.Match(strSQL);
                while (MatchResults.Success)
                {
                    //Group "SQLParam": MatchQtty
                    //Group "Val": 8000
                    string strProp = MatchResults.Groups["SQLParam"].Value.ToString();
                    string strVal = MatchResults.Groups["Val"].Value.ToString();

                    //if (strSymbol == DEBUG_CODE) CLog.LogEx("LE.txt", "(1)strProp=" + strProp);
                    //if (strSymbol == DEBUG_CODE) CLog.LogEx("LE.txt", "(1)strVal=" + strVal);

                    // {"MT":"11:17:15","MQ":20,"MP":48.9,"TQ":10340}
                    switch (strProp)
                    {
                        case LE_MT_LONG:    // "Time":
                            strProp = LE_MT_SHORT;  // "MT"
                            //strVal=strVal // val time giu nguyen
                            break;
                        case LE_MP_LONG:    //"MatchPrice":
                            if (!IsNumeric(strVal)) return "";// ko phai so thi return luon
                            strProp = LE_MP_SHORT;  //"MP"
                            strVal = (Convert.ToDecimal(strVal) / 1000).ToString();     // giu nguyen
                            break;
                        case LE_MQ_LONG:    //"MatchQtty":
                            if (!IsNumeric(strVal)) return "";// ko phai so thi return luon
                            strProp = LE_MQ_SHORT;  // "MQ"
                            strVal = strVal.Replace(".000000", "");// bo so sau dau phay
                            break;
                        case LE_TQ_LONG:    //"NM_TotalTradedQtty":
                            if (!IsNumeric(strVal)) return "";// ko phai so thi return luon
                            strVal = strVal.Replace(".000000", ""); // bo so sau dau phay

                            string strOldNMQtty = Dic_GetValue(this.m_dicLE_NM_TotalTradedQtty, strSymbol, "0");
                            string strNewNMQtty = strVal;

                            // thong tin moi van giong thong tin cu thi exit 
                            if (strOldNMQtty == strNewNMQtty)
                                return "";
                            else
                                Dic_AddOrUpdate(ref this.m_dicLE_NM_TotalTradedQtty, strSymbol, strNewNMQtty);

                            strProp = LE_TQ_SHORT; // "TQ"

                            break;
                    }

                    //if (strSymbol == DEBUG_CODE) CLog.LogEx("LE.txt", "(2)strProp=" + strProp);
                    //if (strSymbol == DEBUG_CODE) CLog.LogEx("LE.txt", "(2)strVal=" + strVal);

                    // noi string json
                    //sbJsonC.Append(",\"").Append(sbProp).Append("\":\"").Append(sbVal).Append("\""); // ,"BBQ1":"3000"
                    sbJsonC = sbJsonC.Replace("(" + strProp + ")", strVal);

                    intMatchCount++;// ko co intMatchCount thi ko the return string jsonC

                    MatchResults = MatchResults.NextMatch();
                }

                //if (strSymbol == DEBUG_CODE) CLog.LogEx("LE.txt", "sbJsonC=" + sbJsonC.ToString ());
                //if (strSymbol == DEBUG_CODE) CLog.LogEx("LE.txt", "intMatchCount=" + intMatchCount.ToString());

                // ko co match thi return ""
                // match time luon ok vay la phai >1
                if (intMatchCount <= 1)
                    return "";

                // return jsonC
                strJsonC = sbJsonC.ToString();

                // return
                return strJsonC;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return "";
            }
        }
        public static Boolean IsNumeric(string stringToTest)
        {
            double result;
            return double.TryParse(stringToTest, out result);
        }
        protected string Dic_GetValue(Dictionary<string, string> dic, string strKey, string strDefaultValue)
        {
            try
            {
                // ko co data nao trong dic , return luon default value
                if (dic.Count == 0)
                    return strDefaultValue;

                string strOut = "";

                // test co value trong dic ko
                if (dic.TryGetValue(strKey, out strOut))
                    return dic[strKey]; // get value from existing key
                else
                    return strDefaultValue;// return strDefaultValue
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return "";
            }
        }
        private string SQL2JsonC_PO(string strSQL, ref string strSymbol)
        {
            try
            {
                const string DEBUG_CODE = "VND";

                // ko phai PO thi exit
                //EXEC prc_S5G_HNX_SAVER_IG_PO_UPDATE @BeginString='HNX.TDS.1',@BodyLength='149',@MsgType='PO',@SenderCompID='HNX',@SendingTime='20160613-09:44:33',@Symbol='VCC',@BoardCode='LIS_BRD_01',@BidPrice_1='16000',@BidQtty_1='47',@BidPrice_2='15900',@BidQtty_2='50',@BidPrice_3='15800',@BidQtty_3='50',@OfferPrice_1='0',@OfferQtty_1='0',@OfferPrice_2='0',@OfferQtty_2='0',@OfferPrice_3='0',@OfferQtty_3='0'
                if (!Regex.IsMatch(strSQL, REGEX_SQL_CHECK_IS_PO))
                    return "";

                //----------------------------------------------------------

                // declare
                //const string TEMPLATE_JSONC_PO = "{\"T\":\"(T)\",\"S\":(S),\"BP1\":(BP1),\"BQ1\":(BQ1),\"BP2\":(BP2),\"BQ2\":(BQ2),\"BP3\":(BP3),\"BQ3\":(BQ3),\"SP1\":(SP1),\"SQ1\":(SQ1),\"SP2\":(SP2),\"SQ2\":(SQ2),\"SP3\":(SP3),\"SQ3\":(SQ3)}";    //
                StringBuilder sbJsonC = new StringBuilder(TEMPLATE_JSONC_PO);
                string strJsonC = "";
                int intMatchCount = 0;

             
                // lay symbol tu strSQL (SI/TP)
                strSymbol = Regex.Match(strSQL, REGEX_SQL_GET_SYMBOL).Groups[1].Value;

                // lay ra tat ca Prop/Val trong strReplacedSQL
                Regex RegexObj = new Regex(REGEX_SQL_GET_PROP_VAL_4_PO);
                Match MatchResults = RegexObj.Match(strSQL);
                if (MatchResults.Success)
                {
                    //"@MsgType='PO'.*?@SendingTime='(?<SendingTime>.*?)',@Symbol='(?<Symbol>.*?)'.*?@BidPrice_1='(?<BidPrice_1>.*?)',@BidQtty_1='(?<BidQtty_1>.*?)',@BidPrice_2='(?<BidPrice_2>.*?)',@BidQtty_2='(?<BidQtty_2>.*?)',@BidPrice_3='(?<BidPrice_3>.*?)',@BidQtty_3='(?<BidQtty_3>.*?)',@OfferPrice_1='(?<OfferPrice_1>.*?)',@OfferQtty_1='(?<OfferQtty_1>.*?)',@OfferPrice_2='(?<OfferPrice_2>.*?)',@OfferQtty_2='(?<OfferQtty_2>.*?)',@OfferPrice_3='(?<OfferPrice_3>.*?)',@OfferQtty_3='(?<OfferQtty_3>.*?)'";
                    string T = MatchResults.Groups["SendingTime"].Value.ToString();
                    string S = MatchResults.Groups["Symbol"].Value.ToString();
                    string BP1 = MatchResults.Groups["BidPrice_1"].Value.ToString();
                    string BQ1 = MatchResults.Groups["BidQtty_1"].Value.ToString();
                    string BP2 = MatchResults.Groups["BidPrice_2"].Value.ToString();
                    string BQ2 = MatchResults.Groups["BidQtty_2"].Value.ToString();
                    string BP3 = MatchResults.Groups["BidPrice_3"].Value.ToString();
                    string BQ3 = MatchResults.Groups["BidQtty_3"].Value.ToString();
                    string SP1 = MatchResults.Groups["OfferPrice_1"].Value.ToString();
                    string SQ1 = MatchResults.Groups["OfferQtty_1"].Value.ToString();
                    string SP2 = MatchResults.Groups["OfferPrice_2"].Value.ToString();
                    string SQ2 = MatchResults.Groups["OfferQtty_2"].Value.ToString();
                    string SP3 = MatchResults.Groups["OfferPrice_3"].Value.ToString();
                    string SQ3 = MatchResults.Groups["OfferQtty_3"].Value.ToString();

                    if (IsNumeric(BP1)) BP1 = (Convert.ToDecimal(BP1) / 1000).ToString();
                    if (IsNumeric(BP2)) BP2 = (Convert.ToDecimal(BP2) / 1000).ToString();
                    if (IsNumeric(BP3)) BP3 = (Convert.ToDecimal(BP3) / 1000).ToString();
                    if (IsNumeric(SP1)) SP1 = (Convert.ToDecimal(SP1) / 1000).ToString();
                    if (IsNumeric(SP2)) SP2 = (Convert.ToDecimal(SP2) / 1000).ToString();
                    if (IsNumeric(SP3)) SP3 = (Convert.ToDecimal(SP3) / 1000).ToString();

                    sbJsonC
                        .Replace("(T)", T)
                        .Replace("(S)", S)
                        .Replace("(BP1)", BP1)
                        .Replace("(BQ1)", BQ1)
                        .Replace("(BP2)", BP2)
                        .Replace("(BQ2)", BQ2)
                        .Replace("(BP3)", BP3)
                        .Replace("(BQ3)", BQ3)
                        .Replace("(SP1)", SP1)
                        .Replace("(SQ1)", SQ1)
                        .Replace("(SP2)", SP2)
                        .Replace("(SQ2)", SQ2)
                        .Replace("(SP3)", SP3)
                        .Replace("(SQ3)", SQ3);

                    intMatchCount++;// ko co intMatchCount thi ko the return string jsonC
                }

                
                // return jsonC
                strJsonC = sbJsonC.ToString();

                // return
                return strJsonC;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return "";
            }
        }

        public bool InsertPO2Redis(string strSQL,CRedis_New m_RC)
        {
            try
            {
                string strSymbol = "", strJsonC = "";

                //
                strJsonC = this.SQL2JsonC_PO(strSQL, ref strSymbol);

                // ko co du lieu thi exit
                if (strJsonC == "") return false;

                // tao key/value
                string strKey = TEMPLATE_REDIS_KEY_PO.Replace("(Symbol)", strSymbol);
                string strValue = strJsonC;

                // update key (string type)
                m_RC.SetCache(strKey, strValue, intPeriod);
               
                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }
        public bool InsertLE2Redis(string strSQL,CRedisConfig redisConfig)
        {
            try
            {
                // Data
                string strSymbol = "", strJsonC = "";

                //{"MT":"11:17:15","MQ":20,"MP":48.9,"TQ":10340}
                strJsonC = this.SQL2JsonC_LE(strSQL, ref strSymbol);

                // ko co du lieu thi exit
                if (strJsonC == "") return false;

                //LE:S5G_ACB
                string Z_KEY = TEMPLATE_REDIS_KEY_LE.Replace("(Symbol)", strSymbol);
                double Z_SCORE = Convert.ToDouble(DateTime.Now.ToString(FORMAT_TIME_5));
                string Z_VALUE = strJsonC;

                //thêm kết nối đến reidis riêng để tránh các error sinh ra do dùng chung 1 kết nối trên nhiều chuỗi - 2022-09-27 trungmq
                // Linhnh cho vào try-catch để tránh trường hợp connect failed 1 redis làm ảnh hưởng đến connect  redis còn lại

                try
                {
                   
                    using (var connectionFox = ConnectionMultiplexer.Connect($"{redisConfig.Host_FOX}:{redisConfig.Port_FOX}"))
                    {
                        var dbFox = connectionFox.GetDatabase();
                        // log redis (CPU rat cao, khong log duoc)
                        //CLog.LogRedis(Z_KEY, Z_SCORE, Z_VALUE);
                        // insert ZSet vao redis
                        dbFox.SortedSetAdd(Z_KEY, Z_VALUE, Z_SCORE);

                    }
                }
                catch (Exception ex)
                {
                    this._app.ErrorLogger.LogError(ex);
                }
                // insert redis Fox  2023-05-22 LinhNH
                try
                {
                    using (var connectionLLQ = ConnectionMultiplexer.Connect($"{redisConfig.Host_LLQ}:{redisConfig.Port_LLQ}"))
                    {
                        var dbLLQ = connectionLLQ.GetDatabase();

                        dbLLQ.SortedSetAdd(Z_KEY, Z_VALUE, Z_SCORE);
                    }
                }
                catch (Exception ex)
                {
                    this._app.ErrorLogger.LogError(ex);
                }

                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }
        public bool InsertRealtimeData2Redis(string strSQL,CRedisConfig redisConfig)
        {
            try
            {
                // Data 
                string strSymbol = "", strType = "", strJsonC = "";

                //{"SI":{}, "TP":{"ST":"20151119-09:00:21","NTP":"2","BBP1":"19900","BBQ1":"3000","BBP2":"18500","BBQ2":"1000"}}
                strJsonC = this.SQL2JsonC(strSQL, ref strType, ref strSymbol);

                // ko co du lieu thi exit
                if (strJsonC == "") return false;

                //REALTIME:S5G_ACB
                string Z_KEY = TEMPLATE_REDIS_KEY_REALTIME
                    .Replace("(Date)", DateTime.Now.ToString(TEMPLATE_REDIS_KEY_REALTIME_DATE))
                    .Replace("(Symbol)", strSymbol);
                double Z_SCORE = Convert.ToDouble(DateTime.Now.ToString(FORMAT_TIME_5));
                string Z_VALUE = strJsonC;

                //thêm kết nối đến reidis riêng để tránh các error sinh ra do dùng chung 1 kết nối trên nhiều chuỗi - 2022-09-27 trungmq
                // Linhnh cho vào try-catch để tránh trường hợp connect failed 1 redis làm ảnh hưởng đến connect  redis còn lại
                // Redis LLQ
                try
                {
                    using (var connectionFox = ConnectionMultiplexer.Connect($"{redisConfig.Host_FOX}:{redisConfig.Port_FOX}"))
                    {
                        var dbFox = connectionFox.GetDatabase(); // Lấy database
                                                                 // Thêm phần tử vào ZSet
                        dbFox.SortedSetAdd(Z_KEY, Z_VALUE, Z_SCORE);

                    }
                }
                catch (Exception ex)
                {
                    this._app.ErrorLogger.LogError(ex);
                }

                // Insert redis Fox 2023-05-22 LinhNH
                try
                {
                    using (var connectionLLQ = ConnectionMultiplexer.Connect($"{redisConfig.Host_LLQ}:{redisConfig.Port_LLQ}"))
                    {
                        var dbLLQ = connectionLLQ.GetDatabase(); // Lấy database
                                                                 // Thêm phần tử vào ZSet
                        dbLLQ.SortedSetAdd(Z_KEY, Z_VALUE, Z_SCORE);

                    }
                }
                catch (Exception ex)
                {
                    this._app.ErrorLogger.LogError(ex);
                }

                return true;

            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }
        private StringBuilder LongVal2ShortVal(StringBuilder sbLongVal)
        {
            try
            {
                return sbLongVal
                    //SendingTime
                    .Replace(".000000", "");
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return sbLongVal;
            }
        }
        private StringBuilder SQLParam2ShortProp(StringBuilder sbSQLParam)
        {
            try
            {
                return sbSQLParam
                    //SendingTime
                    .Replace("SendingTime", "STime")

                    //SI
                    .Replace("MidPx", "AP")
                    .Replace("BoardCode", "BoC")
                    .Replace("BidCount", "BiC")
                    .Replace("BuyCount", "BuC")
                    .Replace("BuyForeignQtty", "BFQ")
                    .Replace("BuyForeignValue", "BFV")
                    .Replace("BasicPrice", "BP")
                    .Replace("CeilingPrice", "CeP")
                    .Replace("ClosePrice", "ClP")
                    .Replace("CurrentPrice", "Cp")
                    .Replace("CurrentQtty", "CQ")
                    .Replace("FloorPrice", "FP")
                    .Replace("HighestPice", "HP")
                    .Replace("IDSymbol", "ID")
                    .Replace("ListingStatus", "LS")
                    .Replace("LowestPrice", "LP")
                    .Replace("MatchPrice", "MP")
                    .Replace("MatchQtty", "MQ")
                    .Replace("NM_TotalTradedQtty", "NMTTQ")
                    .Replace("NM_TotalTradedValue", "NMTTV")
                    .Replace("OfferCount", "OC")
                    .Replace("OpenPrice", "OP")
                    .Replace("PT_MatchPrice", "PTMP")
                    .Replace("PT_MatchQtty", "PTMQ")
                    .Replace("PT_TotalTradedQtty", "PTTTQ")
                    .Replace("PT_TotalTradedValue", "PTTTV")
                    .Replace("RemainForeignQtty", "RFQ")
                    .Replace("ReferenceStatus", "RS")
                    .Replace("SellForeignQtty", "SFQ")
                    .Replace("SellForeignValue", "SFV")
                    .Replace("SecurityType", "ST")
                    .Replace("SecurityTradingStatus", "STS")
                    .Replace("Symbol", "Sym")
                    .Replace("TotalBidQtty", "TBQ")
                    .Replace("TotalBidQtty_OD", "TBQOD")
                    .Replace("TotalBuyTradingQtty", "TBTQ")
                    .Replace("TotalBuyTradingValue", "TBTV")
                    .Replace("Tradingdate", "TD")
                    .Replace("Time", "Time")
                    .Replace("TotalListingQtty", "TLQ")
                    .Replace("TotalOfferQtty", "TOQ")
                    .Replace("TotalOfferQtty_OD", "TOQOD")
                    .Replace("TotalValueTraded", "TVaT")
                    .Replace("TotalVolumeTraded", "TVoT")
                    .Replace("TradingSessionID", "TSID")
                    .Replace("TradSesStatus", "TSS")

                    //TP
                    .Replace("NoTopPrice", "NTP")
                    .Replace("BestBidPrice", "BBP")
                    .Replace("BestBidQtty", "BBQ")
                    .Replace("BestOfferPrice", "BOP")
                    .Replace("BestOfferQtty", "BOQ");
            }
            catch (Exception ex)
            {
               
                return sbSQLParam;
            }
        }
        private string SQL2JsonC(string strSQL, ref string strType, ref string strSymbol)
        {
            try
            {
                // ko phai SI hoac TP thi exit
                if (!Regex.IsMatch(strSQL, REGEX_SQL_CHECK_IS_SI_TP))
                    return "";

                // declare
                StringBuilder sbJsonC = new StringBuilder(TEMPLATE_JSONC);
                StringBuilder sbSI = new StringBuilder("");
                StringBuilder sbTP = new StringBuilder("");
                string strReplacedSQL = "";
                string strJsonC = "";
                string strSendingTime = "";

                // lay type tu strSQL (SI/TP)
                strType = Regex.Match(strSQL, REGEX_SQL_GET_TYPE).Groups[1].Value;

                // lay symbol tu strSQL (SI/TP)
                strSymbol = Regex.Match(strSQL, REGEX_SQL_GET_SYMBOL).Groups[1].Value;

                // lay SendingTime tu strSQL (SI/TP)
                strSendingTime = Regex.Match(strSQL, REGEX_SQL_GET_SENDING_TIME).Groups[1].Value;

                // debug
                //if (strSymbol != "SHB") 
                //    return "";

                // replace de xoa cac thong tin ko can thiet
                strReplacedSQL = Regex.Replace(strSQL, REGEX_SQL_REPLACE_REMOVE_TRASH, "");

                // lay ra tat ca Prop/Val trong strReplacedSQL
                Regex RegexObj = new Regex(REGEX_SQL_GET_PROP_VAL);
                Match MatchResults = RegexObj.Match(strReplacedSQL);
                while (MatchResults.Success)
                {
                    StringBuilder sbProp = new StringBuilder(MatchResults.Groups["SQLParam"].Value);
                    StringBuilder sbVal = new StringBuilder(MatchResults.Groups["Val"].Value);

                    //replace SQLParam dai ngay Prop ngan 
                    sbProp = this.SQLParam2ShortProp(sbProp);
                    sbVal = this.LongVal2ShortVal(sbVal);

                    // noi string json
                    if (strType == TYPE_SI) sbSI.Append(",\"").Append(sbProp).Append("\":\"").Append(sbVal).Append("\""); // ,"BBQ1":"3000"
                    if (strType == TYPE_TP) sbTP.Append(",\"").Append(sbProp).Append("\":\"").Append(sbVal).Append("\""); // ,"BBQ1":"3000"

                    MatchResults = MatchResults.NextMatch();
                }

                // xoa ky , tu dau tien
                if (sbSI.Length > 0) sbSI.Remove(0, 1);
                if (sbTP.Length > 0) sbTP.Remove(0, 1);

                // return jsonC
                sbJsonC
                    .Replace("(STime)", DateTime.Now.ToString(FORMAT_DATETIME_6)) //strSendingTime ko co phan nghin second nen phai dung server time cua FPTS
                    .Replace("(SI)", sbSI.ToString())
                    .Replace("(TP)", sbTP.ToString());

                strJsonC = sbJsonC.ToString();

                // return
                return strJsonC;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return "";
            }
        }
        public string Message2SQL(string strMessage, ref string strResultOracle, ref string strLogon)
        {
            try
            {
                string strType = "";
                string strResult = "";
                string strSPname = "";
                string strSQL = "";
                string[] arrPart = null;

                // replace so thanh chu
                this.ReplaceTags(strMessage, ref strType, ref strResult, ref strResultOracle, ref arrPart, REPLACE_TAGS.FOR_DB);

               
                //Them phan insert Key LogonHNX
                if (strType == MSG_TYPE_DERIVATIVES_INFO_A_LOGIN)
                {
                    strLogon = arrPart[5].Substring(3);
                    return "";
                }

           
                if (strResult == "")
                    return "";

                // tim ra ten SP theo quy tac
                strSPname = TEMPLATE_SP_S5G_HNX_SAVER.Replace("(Type)", strType);

                // obj SB de xu ly toc do nhanh
                StringBuilder sb = new StringBuilder(strResult, strResult.Length * 2);
                StringBuilder sbOracle = new StringBuilder(strResultOracle, strResultOracle.Length * 2);
                // binMsg= 8=HNX.TDS.19=015135=BI49=HNX52=20150922-10:45:22421=LIS_BRD_ETF422=LIS_BRD_ETF17=179425=LIS_BRD_ETF426=A388=20150922399=10:45:22336=LIS_CON_NML340=1341=LIS
                // => 8=HNX.TDS.1","9=0151","35=BI","49=HNX","52=20150922-10:45:22","421=LIS_BRD_ETF","422=LIS_BRD_ETF","17=179","425=LIS_BRD_ETF","426=A","388=20150922","399=10:45:22","336=LIS_CON_NML","340=1","341=LIS","
                sb.Replace(((char)1).ToString(), "',@");
                //2018-07-20 12:26:56 hungpv
                if (strType == MSG_TYPE_DERIVATIVES_INFO)
                {
                    sbOracle.Replace(((char)1).ToString(), "|");
                    strResultOracle = sbOracle.ToString();

                }

                // => 8":"HNX.TDS.1","9":"0151","35":"BI","49":"HNX","52":"20150922-10:45:22","421":"LIS_BRD_ETF","422":"LIS_BRD_ETF","17":"179","425":"LIS_BRD_ETF","426":"A","388":"20150922","399":"10:45:22","336":"LIS_CON_NML","340":"1","341":"LIS","
                sb.Replace("=", "='");

                // => ......,"341":"LIS","  => ,"341":"LIS"
                sb.Length -= 3;

                // => {"BeginString":"HNX.TDS.1","BodyLength":"0
                sb.Insert(0, strSPname + " @");

                // => .....,"TradingSessionID":"LIS_CON_NML","TradSesStatus":"1"}
                sb.Append("'");

                // SQL script
                strSQL = sb.ToString();

                // neu TopNPrice thi dung function rieng
                if (strType == MSG_TYPE_TOP_N_PRICE)
                    return this.SQL2SQLTopNPrice(strSQL);

                // return
                return strSQL;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return "";
            }
        }
        private string SQL2SQLTopNPrice(string strSQL)
        {
            try
            {
                strSQL = strSQL.Replace("@NumTopPrice", "\r\n" + "@NumTopPrice");
                Regex RegexObj = new Regex("(?<FullRow>@NumTopPrice.*)");
                Match MatchResults = default(Match);
                Match MatchResults2 = default(Match);
                string strFullRow = "";
                string strFullRowNew = "";
                string BESTBIDPRICE = "@BestBidPrice";
                string BESTBIDQTTY = "@BestBidQtty";
                string BESTOFFERPRICE = "@BestOfferPrice";
                string BESTOFFERQTTY = "@BestOfferQtty";
                MatchResults = RegexObj.Match(strSQL);

                while (MatchResults.Success)
                {
                    strFullRow = (MatchResults.Groups["FullRow"].Value);
                    MatchResults2 = Regex.Match(strFullRow, "(?<FullNumTop>@NumTopPrice='(?<NumTop>\\d*?)',)");
                    string strNumTop = MatchResults2.Groups["NumTop"].Value;
                    string strFullNumTop = MatchResults2.Groups["FullNumTop"].Value;
                    strFullRowNew = strFullRow.Replace(BESTBIDPRICE, BESTBIDPRICE + strNumTop);
                    strFullRowNew = strFullRowNew.Replace(BESTBIDQTTY, BESTBIDQTTY + strNumTop);
                    strFullRowNew = strFullRowNew.Replace(BESTOFFERPRICE, BESTOFFERPRICE + strNumTop);
                    strFullRowNew = strFullRowNew.Replace(BESTOFFERQTTY, BESTOFFERQTTY + strNumTop);
                    strSQL = strSQL.Replace(strFullRow, strFullRowNew);
                    MatchResults = MatchResults.NextMatch();
                }
                strSQL = strSQL.Replace("\r\n", "");

                strSQL = Regex.Replace(strSQL, "@NumTopPrice.*?,", ""); // xoa param @NumTopPrice vi ko can thiet, gay dai SQL script (SP cung ko co param nay)

                strSQL = RemoveHeader(strSQL, "@");

                return strSQL;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return "";
            }
        }
        private string RemoveHeader(string strInput, string strPrefix)
        {
            try
            {
                string strOutput = strInput;

            
                strOutput = Regex.Replace(strOutput, strPrefix + "BeginString.*?,", "");
                strOutput = Regex.Replace(strOutput, strPrefix + "BodyLength.*?,", "");
                //strOutput = Regex.Replace(strOutput, strPrefix + "MsgType.*?,", "");
                strOutput = Regex.Replace(strOutput, strPrefix + "SenderCompID.*?,", "");
                //strOutput = Regex.Replace(strOutput, strPrefix + "SendingTime.*?,", "");

                strOutput = Regex.Replace(strOutput, strPrefix + "f8\".*?,", "");
                strOutput = Regex.Replace(strOutput, strPrefix + "f9\".*?,", "");
                //strOutput = Regex.Replace(strOutput, strPrefix + "f35\".*?,", "");
                strOutput = Regex.Replace(strOutput, strPrefix + "f49\".*?,", "");
                strOutput = Regex.Replace(strOutput, strPrefix + "f52\".*?,", "");  //=> Can not add Newtonsoft.Json.Linq.JValue to Newtonsoft.Json.Linq.JObject => double "f52" prop => can phai xoa "f52" trong Body

                return strOutput;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return "";
            }
        }
        public bool InsertBI2RedisfromRaw(string rawData,CRedis_New m_RC)
        {
            try
            {
                string msgType = Regex.Match(rawData, "35=(.*?)", RegexOptions.Multiline).Groups[1].Value; // 2022-03-07 hungtq2 tim msgtype raw data
                string strBoardCode = Regex.Match(rawData, "425=(.*?)", RegexOptions.Multiline).Groups[1].Value; // 2022-03-07 hungtq2 tim msgtype raw data
                List<CModelPublic.IG_BI_FULL_NEW> BI = new List<CModelPublic.IG_BI_FULL_NEW>();
                if (msgType != MSG_TYPE_BOARD_INFO)
                    return false;
                string json = Fix2Json(rawData);

                string strKey = REDIS_KEY_HNX_BI;
                Dic_AddOrUpdate(ref m_dicBI2, strBoardCode, json);
                // cho vao list
                foreach (KeyValuePair<string, string> pair in m_dicBI2)
                {
                    // list obj
                    CModelPublic.IG_BI_FULL_NEW BIF = JsonConvert.DeserializeObject<CModelPublic.IG_BI_FULL_NEW>(pair.Value);
                    BI.Add(BIF);
                }
                CModelPublic.EDataSingle eDataSingle = new CModelPublic.EDataSingle(BI);
                string jsonx = JsonConvert.SerializeObject(eDataSingle);
                m_RC.SetCacheBI(strKey, jsonx, intPeriod);
                //string jsonx = JsonConvert.SerializeObject(eDataSingle);
                //this.m_RCFox.SetCacheBI(strKey, eDataSingle, CConfig.intPeriod);
                return true;
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return false;
            }
        }
      
        // nguon data chinh, data dau vao cho app
        public string[] ReadAllMessagesFromQueue()
        {
            try
            {
             
                lock (m_objLocker) // lock multi-thread => Message requested was not found in the queue specified
                {
                    string[] arrstrMsgBody = null;

                    //if this queue doesn't exist >>>> exit
                    if (!MessageQueue.Exists(this.m_strSrcQueuePath))
                        return null;

                    //// init queue / using se tu giai phong obj khi xong
                    using (MessageQueue objMQ = new MessageQueue(this.m_strSrcQueuePath))
                    {
                        // BinaryQueue (IS5 Queue)
                        if (this.m_enmCurrentMessageFormatter == MESSAGE_FORMATTER.BINARY)
                            objMQ.Formatter = new BinaryMessageFormatter();

                        // XML (IG Queue)
                        if (this.m_enmCurrentMessageFormatter == MESSAGE_FORMATTER.XML)
                            objMQ.Formatter = new XmlMessageFormatter(new Type[] { typeof(String) });

                        // Populate an array with copies of all the messages in the queue.
                        // fn nay ko xoa msg trong queue
                        Message[] arrMessage = objMQ.GetAllMessages();

                        // Loop through the messages.
                        if (arrMessage.Length > 0)
                            foreach (Message msg in arrMessage)
                            {
                                // add string vao mang 
                                string strMsgBody = msg.Body.ToString();
                                if (arrstrMsgBody == null)
                                {
                                    arrstrMsgBody = new string[1]; // lan dau thi create instance
                                    arrstrMsgBody[0] = strMsgBody;
                                }
                                else
                                {
                                    Array.Resize(ref arrstrMsgBody, arrstrMsgBody.Length + 1); // lan sau thi tang size array + 1
                                    arrstrMsgBody[arrstrMsgBody.Length - 1] = strMsgBody;
                                }

                                // replicate msg
                                if (this.m_blnRepMsg)
                                    this.ReplicateMessageBinary(this.m_strSrcQueuePathRep, strMsgBody);

                                // 2015-12-23 15:25:31 ngocta2
                                // rep msg sang queue2 (FeederHNX4.5G dung queue2)
                                if (this.m_blnRepMsg2)
                                    this.ReplicateMessageXML(this.m_strSrcQueuePath2, strMsgBody);

                                // 2016-11-10 11:52:39 ngocta2
                                // replicate msg cho queue pro
                                if (this.m_blnRepMsgPro)
                                    this.ReplicateMessageBinary(this.m_strSrcQueuePathPro, strMsgBody);

                                // xoa msg trong queue
                                // http://stackoverflow.com/questions/23227194/how-can-i-remove-messages-from-a-queue
                                objMQ.ReceiveById(msg.Id);
                            }
                    }


                    // tang so luong read msg count
                    if (arrstrMsgBody != null)
                        this.m_lngReadMsgCount += arrstrMsgBody.Length;

                    return arrstrMsgBody;
                }
            }
            catch (Exception ex)
            {
                this._app.ErrorLogger.LogError(ex);
                return null;
            }
        }
        public bool ReplicateMessageXML(string strQueuePath, string strMessageBody)
        {
            try
            {
                //if this queue doesn't exist we will create it
                if (!MessageQueue.Exists(strQueuePath))
                    MessageQueue.Create(strQueuePath);

                // init queue / using se tu giai phong obj khi xong
                using (MessageQueue objMQ = new MessageQueue(strQueuePath))
                {
                    // xu dung BinaryMessageFormatter de luu msg trong replicated queue
                    objMQ.Formatter = new XmlMessageFormatter();

                    // rep queue chi dung binary de toc do nhanh nhat co the
                    using (Message objMsg = new Message())
                    {
                        objMsg.Body = (strMessageBody);
                        objMsg.Recoverable = false; // fast speed
                        objMsg.Formatter = new XmlMessageFormatter();

                        //insert
                        objMQ.Send(objMsg);
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
        public bool ReplicateMessageBinary(string strQueuePath, string strMessageBody)
        {
            try
            {
                //if this queue doesn't exist we will create it
                if (!MessageQueue.Exists(strQueuePath))
                    MessageQueue.Create(strQueuePath);

                // init queue / using se tu giai phong obj khi xong
                using (MessageQueue objMQ = new MessageQueue(strQueuePath))
                {
                    // xu dung BinaryMessageFormatter de luu msg trong replicated queue
                    objMQ.Formatter = new BinaryMessageFormatter();

                    // rep queue chi dung binary de toc do nhanh nhat co the
                    using (Message objMsg = new Message())
                    {
                        objMsg.Body = (strMessageBody);
                        objMsg.Recoverable = false; // fast speed
                        objMsg.Formatter = new BinaryMessageFormatter();

                        //insert
                        objMQ.Send(objMsg);
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
      
    }
}
