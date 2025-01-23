using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.History
{
    public class EHistOrder : EHistBase
    {
        public IEnumerable<EHistItemOrder> Body { get; set; }
    }
}
