using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.History
{
    /// <summary>
    /// exec [StockInfoStore].[dbo].[prc_S5G_HIST_GET_STOCK_PRICE] @Center=1,@Code='ABT',@Type=0,@Date='',@BeginDate='25/01/2021',@EndDate='25/02/2021',@SelectedPage=1,@PageSize=400
    /// </summary>
    public class EHistPrice : EHistBase
    {
        public IEnumerable<EHistItemPrice> Body { get; set; }
    }
}
