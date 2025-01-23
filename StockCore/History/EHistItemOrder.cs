using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.History
{
   public class EHistItemOrder
   {
        public long ID { get; set; }
        public string TRADING_DATE { get; set; }
        public string TRADING_DATE_US { get; set; }
        public string TRADING_DATE_VN { get; set; }
        public string CODE { get; set; }
        public double BID_COUNT { get; set; }
        public double TOTAL_BID_QTTY { get; set; }
        public double OFFER_COUNT { get; set; }
        public double TOTAL_OFFER_QTTY { get; set; }
        public double BID_OFFER { get; set; }
        public double CEILING_PRICE { get; set; }
        public double FLOOR_PRICE { get; set; }
        public double BASIC_PRICE { get; set; }
        public double BEST_BID_PRICE { get; set; }
        public double BEST_BID_QTTY { get; set; }
        public double BEST_OFFER_PRICE { get; set; }
        public double BEST_OFFER_QTTY { get; set; }
        public string Status { get; set; }
    }
}
