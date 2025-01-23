using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
    /// <summary>
    /// 2021-03-02 09:00:57 ngocta2
    /// luu tat ca template url api 
    /// </summary>
    public class ERoutes
    {
        // common
        public const string __VERSION_1 = "v1";
        public const string __ROUTE_VERSION = "ver";  //version
        public const string __ROUTE_IP = "ip";
        public const string __ROUTE_BLANK = "blank";  //blank page - hub

        // param
        public const string __PARAM_LIST = "{list}";
        public const string __PARAM_CLIENTCODE = "{clientcode}";
        public const string __PARAM_ID = "{id}";
        public const string __PARAM_MAXSCORE = "{maxScore}";

        // PriceService       
        public const string __ROUTE_API_V1_COMPANY_ALL                 = "api/" + __VERSION_1 + "/company";
        public const string __ROUTE_API_V1_COMPANY_FILTER              = "api/" + __VERSION_1 + "/company/{list}";
        public const string __ROUTE_API_V1_HIST_INDEX                  = "api/" + __VERSION_1 + "/hist/index";
        public const string __ROUTE_API_V1_HIST_STOCK_PRICE            = "api/" + __VERSION_1 + "/hist/quote/price";
        public const string __ROUTE_API_V1_HIST_STOCK_ORDER            = "api/" + __VERSION_1 + "/hist/quote/order";
        public const string __ROUTE_API_V1_HIST_STOCK_FOREGIN_NM       = "api/" + __VERSION_1 + "/hist/quote/foregin_nm";
        public const string __ROUTE_API_V1_HIST_STOCK_FOREGIN_PT       = "api/" + __VERSION_1 + "/hist/quote/foregin_pt";
        public const string __ROUTE_API_V1_MENU                        = "api/" + __VERSION_1 + "/menu/{type}/{language}";
        public const string __ROUTE_API_V1_MINISTRY                    = "api/" + __VERSION_1 + "/ministry";
        public const string __ROUTE_API_V1_MINISTRY_FILTER             = "api/" + __VERSION_1 + "/ministry/{id}";
        public const string __ROUTE_API_V1_LS_ALL                      = "api/" + __VERSION_1 + "/ls/{symbol}";
        public const string __ROUTE_API_V1_LS                          = "api/" + __VERSION_1 + "/ls/{symbol}/{maxScore}";
        public const string __ROUTE_API_V1_CONFIG                      = "api/" + __VERSION_1 + "/config";
        public const string __ROUTE_API_V1_CW                          = "api/" + __VERSION_1 + "/cw";
        public const string __ROUTE_API_V1_LANGUAGE                    = "api/" + __VERSION_1 + "/language/{language}";
        public const string __ROUTE_API_V1_ACCOUNT                     = "api/" + __VERSION_1 + "/account";
        public const string __ROUTE_API_V1_MARKETWATCH                 = "api/" + __VERSION_1 + "/mw";
        public const string __ROUTE_API_V1_MARKETWATCH_DEFAULT         = "api/" + __VERSION_1 + "/mw/template";
        public const string __ROUTE_API_V1_GET_DSMARKETWATCH           = "api/" + __VERSION_1 + "/mw/{clientcode}";
        public const string __ROUTE_API_V1_STOCK_DSACCOUNT_LOGIN       = "/api/stock/" + __VERSION_1 + "/account/login";
        public const string __ROUTE_API_V1_STOCK_DSMARKETWATCH         = "/api/stock/" + __VERSION_1 + "/mw/template/{client_code}";
        public const string __ROUTE_API_V1_STOCK_DSMARKETWATCH_DEFAULT = "/api/stock/" + __VERSION_1 + "/mwd/template/{client_code}";


        // RootApiService

        // HSXApiService + HNXApiService
        public const string __ROUTE_API_V1_QUOTE        = __VERSION_1 + "/quote/{marketId?}/{boardId?}/{symbol?}";
        public const string __ROUTE_API_V1_INDEX        = __VERSION_1 + "/index/{name?}";
        public const string __ROUTE_API_V1_MARKETSTATUS = __VERSION_1 + "/marketstatus/{marketId?}/{boardId?}";
        public const string __ROUTE_API_V1_BASIC        = __VERSION_1 + "/basic/{symbol?}";
        public const string __ROUTE_API_V1_BASKET       = __VERSION_1 + "/basket/{name?}";
        public const string __ROUTE_API_V1_LASTINDEX    = __VERSION_1 + "/lastindex/{name?}";
        public const string __ROUTE_API_V1_BLANK        = __VERSION_1 + "/blank";
        public const string __ROUTE_API_V1_TRADEDATE    = __VERSION_1 + "/tradedate";
        public const string __ROUTE_API_V1_CONFIG_      = __VERSION_1 + "/config";
        public const string __ROUTE_API_V1_TRANSPORTPROVIDER = __VERSION_1 + "/transportprovider";
    }
}
