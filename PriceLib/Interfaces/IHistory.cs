using StockCore.History;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using SystemCore.Entities;

namespace PriceLib.Interfaces
{
    /*
    context.Response.Write(CRoot2.GetHistIndex(strCenter, strTime, strBeginDate, strEndDate, strSelectedPage, strPageSize));
    context.Response.Write(CRoot2.GetHistStockPrice(strCenter, strCode, strType, strDate, strBeginDate, strEndDate, strSelectedPage, strPageSize));
    context.Response.Write(CRoot2.GetHistStockOrder(strCenter, strCode, strType, strDate, strBeginDate, strEndDate, strSelectedPage, strPageSize));
    context.Response.Write(CRoot2.GetHistStockForeignNM(strCenter, strCode, strType, strDate, strBeginDate, strEndDate, strSelectedPage, strPageSize));
    context.Response.Write(CRoot2.GetHistStockForeignPT(strCenter, strCode, strType, strDate, strBeginDate, strEndDate, strSelectedPage, strPageSize));

    exec [STCADAPTER].[dbo].[prc_S5G_HIST_GET_INDEX] @Center=1,@Time='0',@BeginDate='12/01/2021',@EndDate='25/02/2021',@SelectedPage=1,@PageSize=400
    exec [StockInfoStore].[dbo].[prc_S5G_HIST_GET_STOCK_PRICE] @Center=2,@Code='S99',@Type=0,@Date='',@BeginDate='01/02/2021',@EndDate='01/03/2021',@SelectedPage=1,@PageSize=400
    exec [StockInfoStore].[dbo].[prc_S5G_HIST_GET_STOCK_ORDER] @Center=2,@Code='S99',@Type=0,@Date='',@BeginDate='01/02/2021',@EndDate='01/03/2021',@SelectedPage=1,@PageSize=400
    exec [StockInfoStore].[dbo].[prc_S5G_HIST_GET_STOCK_FOREGIN_NM] @Center=2,@Code='S99',@Type=0,@Date='',@BeginDate='01/02/2021',@EndDate='01/03/2021',@SelectedPage=1,@PageSize=400
    exec [StockInfoStore].[dbo].[prc_S5G_HIST_GET_STOCK_FOREGIN_PT] @Center=2,@Code='S99',@Type=0,@Date='',@BeginDate='01/02/2021',@EndDate='01/03/2021',@SelectedPage=1,@PageSize=400
     */
    public interface IHistory
    {
        //exec [STCADAPTER].[dbo].[prc_S5G_HIST_GET_INDEX] @Center=1,@Time='0',@BeginDate='12/01/2021',@EndDate='25/02/2021',@SelectedPage=1,@PageSize=400
      //  Task<EDalResult> GetHistIndex(EHistSearch histSearch);
        //  exec [StockInfoStore].[dbo].[prc_S5G_HIST_GET_STOCK_PRICE] @Center=2,@Code='S99',@Type=0,@Date='',@BeginDate='01/02/2021',@EndDate='01/03/2021',@SelectedPage=1,@PageSize=400
      //  Task<EDalResult> GetHistQuotePrice(EHistSearch histSearch);
        //  exec [StockInfoStore].[dbo].[prc_S5G_HIST_GET_STOCK_ORDER] @Center=2,@Code='S99',@Type=0,@Date='',@BeginDate='01/02/2021',@EndDate='01/03/2021',@SelectedPage=1,@PageSize=400
      //  Task<EDalResult> GetHistQuoteOrder(EHistSearch histSearch);
        //  exec [StockInfoStore].[dbo].[prc_S5G_HIST_GET_STOCK_FOREGIN_NM] @Center=2,@Code='S99',@Type=0,@Date='',@BeginDate='01/02/2021',@EndDate='01/03/2021',@SelectedPage=1,@PageSize=400
       // Task<EDalResult> GetHistQuoteForeginNM(EHistSearch histSearch);
        //  exec [StockInfoStore].[dbo].[prc_S5G_HIST_GET_STOCK_FOREGIN_PT] @Center=2,@Code='S99',@Type=0,@Date='',@BeginDate='01/02/2021',@EndDate='01/03/2021',@SelectedPage=1,@PageSize=400
      //  Task<EDalResult> GetHistQuoteForeginPT(EHistSearch histSearch);
    }
}
