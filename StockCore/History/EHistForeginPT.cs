using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.History
{
    public class EHistForeginPT : EHistBase
    {
        public IEnumerable<EHistItemForeginPT> Body { get; set; }
    }
}
