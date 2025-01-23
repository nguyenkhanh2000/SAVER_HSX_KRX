using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock5G.HNX.Entities
{
    /// <summary>
    /// 2019-01-15 11:05:54 ngocta2
    /// cac field cua msg co type = SI
    /// </summary>
    public class EMessageSI
    {
        public string f8 { get; set; } // "BeginString");
        public string f9 { get; set; } // "BodyLength");
        public string f35 { get; set; } // "MsgType");
        public string f49 { get; set; } // "SenderCompID");
        public string f52 { get; set; } // "SendingTime");
                                        //'------------------------------
        public string f55 { get; set; } // "Symbol");
        public string f15 { get; set; } // "IDSymbol");
        public string f425 { get; set; } // "BoardCode");
        public string f336 { get; set; } // "TradingSessionID");
        public string f340 { get; set; } // "TradSesStatus");
        public string f326 { get; set; } // "SecurityTradingStatus");
        public string f327 { get; set; } // "ListingStatus");
        public string f167 { get; set; } // "SecurityType");
        public string f225 { get; set; } // "IssueDate");
        public string f106 { get; set; } // "Issuer");
        public string f107 { get; set; } // "SecurityDesc");
        public string f132 { get; set; } // "BestBidPrice");
        public string f1321 { get; set; } // "BestBidQtty");
        public string f133 { get; set; } // "BestOfferPrice");
        public string f1331 { get; set; } // "BestOfferQtty");
        public string f134 { get; set; } // "TotalBidQtty");
        public string f135 { get; set; } // "TotalOfferQtty");
        public string f260 { get; set; } // "BasicPrice");
        public string f333 { get; set; } // "FloorPrice");
        public string f332 { get; set; } // "CeilingPrice");
        public string f334 { get; set; } // "Parvalue");
        public string f31 { get; set; } // "MatchPrice");
        public string f32 { get; set; } // "MatchQtty");
        public string f137 { get; set; } // "OpenPrice");
        public string f138 { get; set; } // "PriorOpenPrice");
        public string f139 { get; set; } // "ClosePrice");
        public string f140 { get; set; } // "PriorClosePrice");
        public string f387 { get; set; } // "TotalVolumeTraded");
        public string f3871 { get; set; } // "TotalValueTraded");
        public string f631 { get; set; } // "MidPx");
        public string f388 { get; set; } // "Tradingdate");
        public string f399 { get; set; } // "Time");
        public string f400 { get; set; } // "TradingUnit");
        public string f109 { get; set; } // "TotalListingQtty");
        public string f17 { get; set; } // "DateNo");
        public string f230 { get; set; } // "AdjustQtty");
        public string f232 { get; set; } // "ReferenceStatus");
        public string f233 { get; set; } // "AdjustRate");
        public string f244 { get; set; } // "DividentRate");
        public string f255 { get; set; } // "CurrentPrice");
        public string f2551 { get; set; } // "CurrentQtty");
        public string f266 { get; set; } // "HighestPice");
        public string f2661 { get; set; } // "LowestPrice");
        public string f277 { get; set; } // "PriorPrice");
        public string f310 { get; set; } // "MatchValue");
        public string f320 { get; set; } // "OfferCount");
        public string f321 { get; set; } // "BidCount");
        public string f391 { get; set; } // "NM_TotalTradedQtty");
        public string f392 { get; set; } // "NM_TotalTradedValue");
        public string f393 { get; set; } // "PT_MatchQtty");
        public string f3931 { get; set; } // "PT_MatchPrice");
        public string f394 { get; set; } // "PT_TotalTradedQtty");
        public string f3941 { get; set; } // "PT_TotalTradedValue");
        public string f395 { get; set; } // "TotalBuyTradingQtty");
        public string f3951 { get; set; } // "BuyCount");
        public string f3952 { get; set; } // "TotalBuyTradingValue");
        public string f396 { get; set; } // "TotalSellTradingQtty");
        public string f3961 { get; set; } // "SellCount");
        public string f3962 { get; set; } // "TotalSellTradingValue");
        public string f397 { get; set; } // "BuyForeignQtty");
        public string f3971 { get; set; } // "BuyForeignValue");
        public string f398 { get; set; } // "SellForeignQtty");
        public string f3981 { get; set; } // "SellForeignValue");
        public string f3301 { get; set; } // "RemainForeignQtty");
        public string f541 { get; set; } // "MaturityDate");
        public string f223 { get; set; } // "CouponRate");
        public string f1341 { get; set; } // "TotalBidQtty_OD");
        public string f1351 { get; set; } // "TotalOfferQtty_OD");
        public string f13 { get; set; } // "fx13");        // MatchChange                      
        public string f3331 { get; set; } // "FloorPricePT");      // Giá sàn cho giao dịch thỏa thuận ngoài biên độ (nghiệp vụ)
        public string f3321 { get; set; } // "CeilingPricePT");   // Giá trần cho giao dịch thỏa thuận ngoài biên độ (nghiệp vụ)


        public string fx13 { get; set; } // tu tinh them
        public string fx29 { get; set; } // tu tinh them
        public string fx30 { get; set; } // tu tinh them
    }
}
