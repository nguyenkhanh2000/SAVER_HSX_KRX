using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.History
{
    // 2021-02-25 15:51:55 ngocta2
    // exec [STCADAPTER].[dbo].[prc_S5G_HIST_GET_INDEX] @Center=1,@Time='0',@BeginDate='12/01/2021',@EndDate='25/02/2021',@SelectedPage=1,@PageSize=400
    public class EHistIndex : EHistBase
    {
        public IEnumerable<EHistItemIndex> Body { get; set; }
    }
}
