using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.History
{
    public class EHistForeginNM : EHistBase
    {
        public IEnumerable<EHistItemForeginNM> Body { get; set; }
    }
}
