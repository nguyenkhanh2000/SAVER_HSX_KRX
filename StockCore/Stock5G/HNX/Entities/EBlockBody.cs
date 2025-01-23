using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock5G.HNX.Entities
{
    public class EBlockBody
    {
        public EHeader Header { get; set; }
        public object Body { get; set; } // Body ko ro la loai msg nao, co the la SI, DI, TP,  .....
    }
}
