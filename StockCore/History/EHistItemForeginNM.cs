using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.History
{
    public class EHistItemForeginNM
    {
        public long IDX { get; set; }
        public string Date { get; set; }
        public string DateVN { get; set; }
        public string Code { get; set; }
        public double CURRENT_VOL { get; set; }
        public double CURRENT_PER { get; set; }
        public double CURRENT_REM { get; set; }
        public double BUY_VOL { get; set; }
        public double BUY_PER { get; set; }
        public double BUY_VAL { get; set; }
        public double BUY_VALPER { get; set; }
        public double SELL_VOL { get; set; }
        public double SELL_PER { get; set; }
        public double SELL_VAL { get; set; }
        public double SELL_VALPER { get; set; }
        public string Status { get; set; }
    }
}
