using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock5G.HNX.Entities
{
    public class EJsonAMessage
    {
        public string BlockHeader { get; set; }
        public List<EBlockBody> BlockBody { get; set; }
    }
}
