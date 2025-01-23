using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.History
{
    /// <summary>
    /// RecordCount	PageSize	PageCount	Range
    //  19	        400	        1	        1 - 19
    /// </summary>
    public class EHistBaseHeader
    {
        public int RecordCount { get; set; }
        public int PageSize { get; set; }
        public int PageCount { get; set; }
        public string Range { get; set; }
    }
}
