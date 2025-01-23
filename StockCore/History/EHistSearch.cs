using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.History
{
    /// <summary>
    /// chua cac dieu kien tim kiem tren form
    /*
    context.Response.Write(CRoot2.GetHistIndex(strCenter, strTime, strBeginDate, strEndDate, strSelectedPage, strPageSize));
    context.Response.Write(CRoot2.GetHistStockPrice(strCenter, strCode, strType, strDate, strBeginDate, strEndDate, strSelectedPage, strPageSize));
    context.Response.Write(CRoot2.GetHistStockOrder(strCenter, strCode, strType, strDate, strBeginDate, strEndDate, strSelectedPage, strPageSize));
    context.Response.Write(CRoot2.GetHistStockForeignNM(strCenter, strCode, strType, strDate, strBeginDate, strEndDate, strSelectedPage, strPageSize));
    context.Response.Write(CRoot2.GetHistStockForeignPT(strCenter, strCode, strType, strDate, strBeginDate, strEndDate, strSelectedPage, strPageSize));
     */
    /// </summary>
    public class EHistSearch
    {
        public string Name { get; set; } // name : vn30
        public int Center { get; set; } // market : HO, HA, UP
        public string Code { get; set; } // symbol : FPT, ACB, ABI
        public int Time { get; set; } // phien: 1,2,3,4
        public int Type { get; set; }   // loai chung khoan: co phieu, trai phieu, chung chi quy
        public string Date { get; set; }// tim trong 1 ngay
        public string BeginDate { get; set; } // tim tu ngay
        public string EndDate { get; set; } //  tim den ngay
        public int SelectedPage { get; set; } // trang can xem
        public int PageSize { get; set; } // so record moi trang (phan trang trong sp, ko return all data ve controller de phan trang tai day)
    }
}
