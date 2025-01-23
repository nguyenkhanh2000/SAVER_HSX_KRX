using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.History
{
    /// <summary>
    //IDX ID  tblID TranDate    TranDateUS Time    IndexValue TotalTrade  TotalShares TotalValues IndexChange IndexChangePercent  ValuesChange ValuesChangePercent SharesChange SharesChangePercent
    //2	20211	20211	25/02/2021	02/25/2021	3	1165.43	285934	508740491	13274024000000	3.42	0.29431760483989	-1594709000000	-10.725251438707	-89903275	-15.0178253088165
    /// </summary>
    public class EHistItemIndex
    {
        public long IDX { get; set; }
        public long ID { get; set; }
        public long tblID { get; set; }
        public string TranDate { get; set; }
        public string TranDateUS { get; set; }
        public int Time { get; set; }
        public double IndexValue { get; set; }
        public int TotalTrade { get; set; }
        public long TotalShares { get; set; }
        public long TotalValues { get; set; }
        public double IndexChange { get; set; }
        public double IndexChangePercent { get; set; }
        public long ValuesChange { get; set; }
        public double ValuesChangePercent { get; set; }
        public double SharesChange { get; set; }
        public long SharesChangePercent { get; set; }
    }
}
