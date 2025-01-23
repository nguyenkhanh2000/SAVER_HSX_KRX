using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock5G.HNX.Entities
{
    /// <summary>
    /// 2019-01-15 15:36:19 ngocta2
    /// phai sinh
    /// </summary>
    public class EMessageDI
    {
        public string f8 { get; set; } // "BeginString");
        public string f9 { get; set; } // "BodyLength");
        public string f35 { get; set; } // "MsgType");
        public string f49 { get; set; } // "SenderCompID");
        public string f52 { get; set; } // "SendingTime");
                                        //'------------------------------
        public string f55 { get; set; } // "Symbol");                    //2018-05-30 09:31:55 ngocta2
        public string f15 { get; set; } // "SymbolID");
        public string f800 { get; set; } // "Underlying");
        public string f425 { get; set; } // "BoardCode");
        public string f336 { get; set; } // "TradingSessionID");
        public string f340 { get; set; } // "TradeSesStatus");
        public string f326 { get; set; } // "SecurityTradingStatus");
        public string f327 { get; set; } // "ListingStatus");
        public string f167 { get; set; } // "SecurityType");
        public string f801 { get; set; } // "OpenInterest");
        public string f8011 { get; set; } // "OpenInterestChange");
        public string f802 { get; set; } // "FirstTradingDate");		// => cal fu name
        public string f803 { get; set; } // "LastTradingDate");			// => cal fu name
		public string f132 { get; set; } // "BestBidPrice");
        public string f1321 { get; set; } // "BestBidQtty");
        public string f133 { get; set; } // "BestOfferPrice");
        public string f1331 { get; set; } // "BestOfferQtty");
        public string f134 { get; set; } // "TotalBidQtty");
        public string f135 { get; set; } // "TotalOfferQtty");
        public string f260 { get; set; } // "BasicPrice");
        public string f333 { get; set; } // "FloorPrice");
        public string f332 { get; set; } // "CeilingPrice");
        public string f31 { get; set; } // "MatchPrice");
        public string f32 { get; set; } // "MatchQtty");
        public string f137 { get; set; } // "OpenPrice");
        public string f138 { get; set; } // "PriorOpenPrice");
        public string f804 { get; set; } // "OpenQtty");
        public string f139 { get; set; } // "ClosePrice");
        public string f140 { get; set; } // "PriorClosePrice");
        public string f805 { get; set; } // "CloseQtty");
        public string f387 { get; set; } // "TotalVolumeTraded");
        public string f3871 { get; set; } // "TotalValueTraded");
        public string f388 { get; set; } // "Tradingdate");
        public string f399 { get; set; } // "Time");
        public string f400 { get; set; } // "TradingUnit");
        public string f17 { get; set; } // "DateNo");
        public string f255 { get; set; } // "CurrentPrice");
        public string f2551 { get; set; } // "CurrentQtty");
        public string f266 { get; set; } // "HighestPrice");
        public string f2661 { get; set; } // "LowestPrice");
        public string f310 { get; set; } // "MatchValue");
        public string f320 { get; set; } // "OfferCount");
        public string f321 { get; set; } // "BidCount");
        public string f391 { get; set; } // "NM_TotalTradedQtty");
        public string f392 { get; set; } // "NM_TotalTradedValue");
        public string f393 { get; set; } // "PT_MatchQtty");
        public string f3931 { get; set; } // "PT_MatchPrice");
        public string f394 { get; set; } // "PT_TotalTradedQtty");
        public string f3941 { get; set; } // "PT_TotalTradedValue");
        public string f814 { get; set; } // "NM_BuyForeignQtty");
        public string f815 { get; set; } // "PT_BuyForeignQtty");
        public string f397 { get; set; } // "BuyForeignQtty");
        public string f8141 { get; set; } // "NM_BuyForeignValue");
        public string f8151 { get; set; } // "PT_BuyForeignValue");
        public string f3971 { get; set; } // "BuyForeignValue");
        public string f816 { get; set; } // "NM_SellForeignQtty");
        public string f817 { get; set; } // "PT_SellForeignQtty");
        public string f398 { get; set; } // "SellForeignQtty");
        public string f8161 { get; set; } // "NM_SellForeignValue");
        public string f8171 { get; set; } // "PT_SellForeignValue");
        public string f3981 { get; set; } // "SellForeignValue");

        public string fx12 { get; set; } // tu tinh them
        public string fx13 { get; set; } // tu tinh them
        public string fx28 { get; set; } // tu tinh them
    }
}
