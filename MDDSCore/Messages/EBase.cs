using Newtonsoft.Json;
using System;

namespace MDDSCore.Messages
{
    public abstract class EBase
    {
        public const string __STRING_FIX_SEPARATOR = "";
        public const string __STRING_EQUAL = "=";
        

        // ==========================================================================================================
        // ===================================== MsyType(35) ========================================================
        // ==========================================================================================================
        /*
        d = Security Definition 
        f = Secrity Status 
        X = Price 
        W = Price Recovery 
        M1 = Index 
        M2 = Investor per Industry 
        M3 = Investor per Symbol 
        M4 = Top N Members per Symbol 
        M7 = Security Information Notification 
        M8 = Symbol Closing Information 
        MA = Open Interest 
        MD = Volatility Interruption 
        ME = Deem Trade Price 
        MF = Foreigner Order Limit 
        MH = Market Maker Information 
        MI = Symbol Event 
        MX = Price Limit Expansion 
        MW = Random End 
        ML = Index Constituents Information 
        MM = ETF iNav 
        MN = ETF iIndex 
        MO = ETF TrackingError 
        MP = Top N Symbols with Trading Quantity 
        MQ = Top N Symbols in Current Price 
        MR = Top N Symbols with High Ratio of Price 
        MS = Top N Symbols with Low Ratio of Price 
        MT = Trading Result of Foreign Investors 
        MU = Disclosure 
        MV = Time Stamp (Polling)
        */
        public const string __MSG_TYPE_SECURITY_DEFINITION                    = "d";
        public const string __MSG_TYPE_SECURITY_STATUS                        = "f";
        public const string __MSG_TYPE_PRICE                                  = "X";
        public const string __MSG_TYPE_PRICE_RECOVERY                         = "W";
        public const string __MSG_TYPE_INDEX                                  = "M1";
        public const string __MSG_TYPE_INVESTOR_PER_INDUSTRY                  = "M2";
        public const string __MSG_TYPE_INVESTOR_PER_SYMBOL                    = "M3";
        public const string __MSG_TYPE_TOP_N_MEMBERS_PER_SYMBOL               = "M4";
        public const string __MSG_TYPE_SECURITY_INFORMATION_NOTIFICATION      = "M7";
        public const string __MSG_TYPE_SYMBOL_CLOSING_INFORMATION             = "M8";
        public const string __MSG_TYPE_OPEN_INTEREST                          = "MA";
        public const string __MSG_TYPE_VOLATILITY_INTERRUPTION                = "MD";
        public const string __MSG_TYPE_DEEM_TRADE_PRICE                       = "ME";
        public const string __MSG_TYPE_FOREIGNER_ORDER_LIMIT                  = "MF";
        public const string __MSG_TYPE_MARKET_MAKER_INFORMATION               = "MH";
        public const string __MSG_TYPE_SYMBOL_EVENT                           = "MI";
        public const string __MSG_TYPE_PRICE_LIMIT_EXPANSION                  = "MX";
        public const string __MSG_TYPE_RANDOM_END                             = "MW";
        public const string __MSG_TYPE_INDEX_CONSTITUENTS_INFORMATION         = "ML";
        public const string __MSG_TYPE_ETF_INAV                               = "MM";
        public const string __MSG_TYPE_ETF_IINDEX                             = "MN";
        public const string __MSG_TYPE_ETF_TRACKINGERROR                      = "MO";
        public const string __MSG_TYPE_TOP_N_SYMBOLS_WITH_TRADING_QUANTITY    = "MP";
        public const string __MSG_TYPE_TOP_N_SYMBOLS_IN_CURRENT_PRICE         = "MQ";
        public const string __MSG_TYPE_TOP_N_SYMBOLS_WITH_HIGH_RATIO_OF_PRICE = "MR";
        public const string __MSG_TYPE_TOP_N_SYMBOLS_WITH_LOW_RATIO_OF_PRICE  = "MS";
        public const string __MSG_TYPE_TRADING_RESULT_OF_FOREIGN_INVESTORS    = "MT";
        public const string __MSG_TYPE_DISCLOSURE                             = "MU";
        public const string __MSG_TYPE_TIME_STAMP_POLLING                     = "MV";
        public const string __MSG_TYPE_DATA_ET                                = "W";
        public const string __MSG_TYPE_DRV_PRODUCT_EVENT                      = "MJ";



        // ==========================================================================================================
        // ===================================== Tags ===============================================================
        // ==========================================================================================================
        public const string __TAG_8     = "8";
        public const string __TAG_9     = "9";
        public const string __TAG_10    = "10";
        public const string __TAG_15    = "15";
        public const string __TAG_30    = "30";
        public const string __TAG_34    = "34";
        public const string __TAG_35    = "35";
        public const string __TAG_40    = "40";
        public const string __TAG_49    = "49";
        public const string __TAG_52    = "52";
        public const string __TAG_55    = "55";
        public const string __TAG_56    = "56";
        public const string __TAG_60    = "60";
        public const string __TAG_75    = "75";
        public const string __TAG_83    = "83";
        public const string __TAG_99    = "99";
        public const string __TAG_106   = "106";
        public const string __TAG_140   = "140";
        public const string __TAG_148   = "148";
        public const string __TAG_200   = "200";
        public const string __TAG_201   = "201";
        public const string __TAG_202   = "202";
        public const string __TAG_207   = "207";
        public const string __TAG_223   = "223";
        public const string __TAG_225   = "225";
        public const string __TAG_231   = "231";
        public const string __TAG_236   = "236";
        public const string __TAG_268   = "268";
        public const string __TAG_269   = "269";
        public const string __TAG_270   = "270";
        public const string __TAG_271   = "271";
        public const string __TAG_279   = "279";
        public const string __TAG_290   = "290";
        public const string __TAG_330   = "330";
        public const string __TAG_331   = "331";
        public const string __TAG_336   = "336";
        public const string __TAG_346   = "346";
        public const string __TAG_381   = "381";
        public const string __TAG_387   = "387";
        public const string __TAG_541   = "541";
        public const string __TAG_911   = "911";
        public const string __TAG_965   = "965";
        public const string __TAG_1148  = "1148";
        public const string __TAG_1149  = "1149";
        public const string __TAG_1193  = "1193";
        public const string __TAG_1194  = "1194";
        public const string __TAG_20000 = "20000";
        public const string __TAG_20003 = "20003";
        public const string __TAG_20004 = "20004";
        public const string __TAG_20005 = "20005";
        public const string __TAG_20008 = "20008";
        public const string __TAG_20009 = "20009";
        public const string __TAG_20013 = "20013";
        public const string __TAG_20014 = "20014";
        public const string __TAG_20015 = "20015";
        public const string __TAG_20016 = "20016";
        public const string __TAG_20018 = "20018";
        public const string __TAG_20020 = "20020";
        public const string __TAG_20023 = "20023";
        public const string __TAG_20026 = "20026";
        public const string __TAG_20027 = "20027";
        public const string __TAG_20030 = "20030";
        public const string __TAG_20031 = "20031";
        public const string __TAG_20032 = "20032";
        public const string __TAG_20033 = "20033";
        public const string __TAG_20034 = "20034";
        public const string __TAG_20047 = "20047";
        public const string __TAG_20048 = "20048";
        public const string __TAG_20054 = "20054";
        public const string __TAG_30001 = "30001";
        public const string __TAG_30167 = "30167";
        public const string __TAG_30168 = "30168";
        public const string __TAG_30169 = "30169";
        public const string __TAG_30213 = "30213";
        public const string __TAG_30214 = "30214";
        public const string __TAG_30215 = "30215";
        public const string __TAG_30216 = "30216";
        public const string __TAG_30217 = "30217";
        public const string __TAG_30270 = "30270";
        public const string __TAG_30271 = "30271";
        public const string __TAG_30301 = "30301";
        public const string __TAG_30511 = "30511";
        public const string __TAG_30512 = "30512";
        public const string __TAG_30521 = "30521";
        public const string __TAG_30522 = "30522";
        public const string __TAG_30523 = "30523";
        public const string __TAG_30524 = "30524";
        public const string __TAG_30540 = "30540";
        public const string __TAG_30541 = "30541";
        public const string __TAG_30552 = "30552";
        public const string __TAG_30553 = "30553";
        public const string __TAG_30554 = "30554";
        public const string __TAG_30557 = "30557";
        public const string __TAG_30558 = "30558";
        public const string __TAG_30561 = "30561";
        public const string __TAG_30562 = "30562";
        public const string __TAG_30563 = "30563";
        public const string __TAG_30565 = "30565";
        public const string __TAG_30566 = "30566";
        public const string __TAG_30567 = "30567";
        public const string __TAG_30568 = "30568";
        public const string __TAG_30569 = "30569";
        public const string __TAG_30573 = "30573";
        public const string __TAG_30574 = "30574";
        public const string __TAG_30575 = "30575";
        public const string __TAG_30576 = "30576";
        public const string __TAG_30577 = "30577";
        public const string __TAG_30578 = "30578";
        public const string __TAG_30589 = "30589";
        public const string __TAG_30590 = "30590";
        public const string __TAG_30591 = "30591";
        public const string __TAG_30592 = "30592";
        public const string __TAG_30593 = "30593";
        public const string __TAG_30594 = "30594";
        public const string __TAG_30595 = "30595";
        public const string __TAG_30596 = "30596";
        public const string __TAG_30597 = "30597";
        public const string __TAG_30599 = "30599";
        public const string __TAG_30600 = "30600";
        public const string __TAG_30602 = "30602";
        public const string __TAG_30603 = "30603";
        public const string __TAG_30604 = "30604";
        public const string __TAG_30605 = "30605";
        public const string __TAG_30606 = "30606";
        public const string __TAG_30607 = "30607";
        public const string __TAG_30608 = "30608";
        public const string __TAG_30609 = "30609";
        public const string __TAG_30610 = "30610";
        public const string __TAG_30611 = "30611";
        public const string __TAG_30612 = "30612";
        public const string __TAG_30613 = "30613";
        public const string __TAG_30614 = "30614";
        public const string __TAG_30615 = "30615";
        public const string __TAG_30616 = "30616";
        public const string __TAG_30617 = "30617";
        public const string __TAG_30618 = "30618";
        public const string __TAG_30619 = "30619";
        public const string __TAG_30620 = "30620";
        public const string __TAG_30621 = "30621";
        public const string __TAG_30622 = "30622";
        public const string __TAG_30623 = "30623";
        public const string __TAG_30624 = "30624";
        public const string __TAG_30625 = "30625";
        public const string __TAG_30628 = "30628";
        public const string __TAG_30629 = "30629";
        public const string __TAG_30630 = "30630";
        public const string __TAG_30631 = "30631";
        public const string __TAG_30632 = "30632";
        public const string __TAG_30633 = "30633";
        public const string __TAG_30634 = "30634";
        public const string __TAG_30635 = "30635";
        public const string __TAG_30636 = "30636";
        public const string __TAG_30637 = "30637";
        public const string __TAG_30638 = "30638";
        public const string __TAG_30639 = "30639";
        public const string __TAG_30640 = "30640";
        public const string __TAG_30641 = "30641";
        public const string __TAG_30642 = "30642";
        public const string __TAG_30643 = "30643";
        public const string __TAG_30644 = "30644";
        public const string __TAG_30645 = "30645";
        public const string __TAG_30646 = "30646";
        public const string __TAG_30647 = "30647";
        public const string __TAG_30648 = "30648";
        public const string __TAG_30651 = "30651";
        public const string __TAG_30652 = "30652";




        // ==========================================================================================================
        // ===================================== Header/Trailer =====================================================
        // ==========================================================================================================

        /// <summary>
        /// 2020-05-04 13:14:52 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=8; required=Y; format=String; length=7</i></para>
        /// <para><b>FIX.4.4</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_8, Order = 1)]
        public string BeginString { get; set; }

        /// <summary>
        /// 2020-05-04 13:14:52 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=9; required=Y; format=Int; length=6</i></para>
        /// <para><b>Độ dài của message</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_9, Order = 2)]
        public long BodyLength { get; set; }

        /// <summary>
        /// 2020-05-04 13:14:52 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=35; required=Y; format=String; length=3</i></para>
        /// <para><b>Loại message, tham chiếu đến mục ”Specification of MsyType(35)” bên dưới</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_35, Order = 3)]
        public string MsgType { get; set; }

        /// <summary>
        /// 2020-05-04 13:14:52 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=49; required=Y; format=String; length=16</i></para>
        /// <para><b>Thông tin bên gửi.</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_49, Order = 4)]
        public string SenderCompID { get; set; }

        /// <summary>
        /// 2020-05-04 13:14:52 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=56; required=Y; format=String; length=16</i></para>
        /// <para><b>9999</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_56, Order = 5)]
        public string TargetCompID { get; set; }

        /// <summary>
        /// 2020-05-04 13:14:52 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=34; required=Y; format=Int; length=11</i></para>
        /// <para><b>Số thứ tự.</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_34, Order = 6)]
        public long MsgSeqNum { get; set; }

        /// <summary>
        /// 2020-05-04 13:14:52 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=52 ; required=Y; format=UTCTime; length=21</i></para>
        /// <para><b>Thời gian gửi.</b></para>
        /// 2020-07-28 14:16:14 ngocta2 => chu y luc set thi la string nhung luc get la DateTime
        /// </summary>
        [JsonProperty(PropertyName = __TAG_52, Order = 7)]
        public string SendingTime { get; set; }

        // ==========================================================================================================
        // ......... body ...............
        // ==========================================================================================================

        /// <summary>
		/// 2020-05-04 13:14:52 hungtq
		/// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
		/// <para><i>tag=10 ; required=Y; format=String; length=3 </i></para>
		/// <para><b>CheckSum có 3 ký tự </b></para>
		/// </summary>
        [JsonProperty(PropertyName = __TAG_10, Order = 1000)]
        public string CheckSum { get; set; }

        /// <summary>
        /// constructor 1
        /// </summary>
        /// <param name="rawData"></param>
        public EBase(string rawData)
        {
            if (rawData == null)
                return;

            string[] arr = rawData.Split(__STRING_FIX_SEPARATOR);
            foreach (string pair in arr)
            {
                if (!string.IsNullOrEmpty(pair))
                {
                    string[] parts = pair.Split(__STRING_EQUAL);
                    switch (parts[0])
                    {
                        case EBase.__TAG_8  : this.BeginString  = parts[1];                  break;
                        case EBase.__TAG_9  : this.BodyLength   = Convert.ToInt64(parts[1]); break;
                        case EBase.__TAG_35 : this.MsgType      = parts[1];                  break;
                        case EBase.__TAG_49 : this.SenderCompID = parts[1];                  break;
                        case EBase.__TAG_56 : this.TargetCompID = parts[1];                  break;
                        case EBase.__TAG_34 : this.MsgSeqNum    = Convert.ToInt64(parts[1]); break;
                        case EBase.__TAG_52 : this.SendingTime  = parts[1];                  break;
                        ////====================================================================================
                        // body ....
                        ////====================================================================================
                        case EBase.__TAG_10 : this.CheckSum     = parts[1];                  break;
                    }
                }
            }
        }

        /// <summary>
        /// constructor 2
        /// </summary>
        /// <param name="rawData"></param>
        public EBase()
        {           
        }
    }
}
