using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.History
{
    public class EHistItemPrice
    {
        public long ID { get; set; }
        public string TranDate { get; set; }
        public string TranDateUS { get; set; }
        public string TranDateVN { get; set; }
        public string StockSymbol { get; set; }
        public double Ceiling { get; set; }
        public double Floor { get; set; }
        public double Basic { get; set; }
        public double Open { get; set; }
        public double Close { get; set; }
        public double Highest { get; set; }
        public double Lowest { get; set; }
        public double ChangePrice { get; set; }
        public double PercentPrice { get; set; }
        public double AP { get; set; }
        public double NTQ { get; set; }
        public double NTV { get; set; }
        public double PTQ { get; set; }
        public double PTV { get; set; }
        public double TTQ { get; set; }
        public double TTV { get; set; }
        public string Status { get; set; }
    }
}
