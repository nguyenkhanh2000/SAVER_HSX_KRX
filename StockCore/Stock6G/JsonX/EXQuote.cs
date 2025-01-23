using Newtonsoft.Json;
using System;
using System.Runtime.Serialization;
using SystemCore.Entities;

namespace StockCore.Stock6G.JsonX
{
    /// <summary>
    /// 2020-08-19 13:29:46 ngocta2
    /// struct quote 
    /// https://liveprice.fpts.com.vn/hnx/data.ashx?s=quote&l=ACB
    /// https://liveprice.fpts.com.vn/hsx/data.ashx?s=quote&l=ABT
    /// 8=FIX.4.49=139235=X49=VNMGW56=9999934=252=20190517 09:00:00.01030001=BDO20004=G7336=9955=VN0ANC11601675=2019022160=09000000130521=030522=030523=030524=0268=2083=1279=0269=1290=1270=0.0271=0346=030270=0.030271=083=2279=0269=0290=1270=0.0271=0346=030270=0.030271=083=3279=0269=1290=2270=0.0271=0346=030270=0.030271=083=4279=0269=0290=2270=0.0271=0346=030270=0.030271=083=5279=0269=1290=3270=0.0271=0346=030270=0.030271=083=6279=0269=0290=3270=0.0271=0346=030270=0.030271=083=7279=0269=1290=4270=0.0271=0346=030270=0.030271=083=8279=0269=0290=4270=0.0271=0346=030270=0.030271=083=9279=0269=1290=5270=0.0271=0346=030270=0.030271=083=10279=0269=0290=5270=0.0271=0346=030270=0.030271=083=11279=0269=1290=6270=0.0271=0346=030270=0.030271=083=12279=0269=0290=6270=0.0271=0346=030270=0.030271=083=13279=0269=1290=7270=0.0271=0346=030270=0.030271=083=14279=0269=0290=7270=0.0271=0346=030270=0.030271=083=15279=0269=1290=8270=0.0271=0346=030270=0.030271=083=16279=0269=0290=8270=0.0271=0346=030270=0.030271=083=17279=0269=1290=9270=0.0271=0346=030270=0.030271=083=18279=0269=0290=9270=0.0271=0346=030270=0.030271=083=19279=0269=1290=10270=0.0271=0346=030270=0.030271=083=20279=0269=0290=10270=0.0271=0346=030270=0.030271=010=167
    /// 8=FIX.4.49=43035=X49=VNMGW56=9999934=385652=20190517 09:59:31.73730001=BDO20004=G1336=4055=VN610313163075=2019022160=095931732387=39420381=5181018570.0268=483=1279=0269=2290=0270=124365.0271=530346=030270=-09999.99990083=2279=0269=4290=0270=134453.0271=0346=030270=-09999.99990083=3279=0269=7290=0270=135528.0271=0346=030270=-09999.99990083=4279=0269=8290=0270=123399.0271=0346=030270=-09999.99990010=218
    /// 
    /// MarketID + BoardID + Symbol (ISIN) la 3 key cho dic nen ko dc bo qua data nay
    /// 
    /// </summary>
    [JsonObject(MemberSerialization.OptIn)] // debug thi comment de show all props, debug xong thi uncomment de hide props
    public class EXQuote
    {
        /// <summary>
        /// khong serialize truong nay
        /// (string -12 char length) VN000000ADS0
        /// The unique code of an issue (a.k.a. instrument or 
        /// symbol). Generally it is ISIN code but in case of
        // Repo market it is defined as the exchange staffs want
        /// </summary>
        [JsonProperty(PropertyName = "isin", Order = 0)]
        [DataMember(Name = "aa")]
        public string ISIN { get; set; }

        /// <summary>
        /// 2020-08-17 14:59:15 ngocta2
        /// symbol
        /// d	TickerCode	30624	CII	VN000000CII6-CII
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30624; required=Y; format=String; length=20</i></para>
        /// <para><b>Mã chứng khoán, (trading code, local code, stock code, bond code và v.v ...)</b></para>
        /// </summary>
        [JsonProperty(PropertyName = "s", Order = 1)]
        [DataMember(Name = "ab")]
        public string Symbol { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
        [DataMember(Name = "ac")]
        public long SymbolSeqNum { get; set; }

        /// <summary>
        /// 2020-08-17 15:01:15 ngocta2
        /// market
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30001; required=Y; format=String; length=3</i></para>
        /// <para><b>ID xác định các thị trường</b></para>
        /// <para>
        ///  Sở giao dịch chứng khoán Tp Hồ Chí Minh <br></br>
        /// - STO: Thị trường cổ phiếu<br></br> 
        /// - BDO: Thị trường trái phiếu<br></br> 
        /// - RPO: Thị trường Repo
        /// </para>
        /// <para>
        ///  Sở giao dịch chứng khoán Hà Nội<br></br>
        /// - STX: Thị trường cổ phiếu<br></br> 
        /// - BDX: Thị trường trái phiếu chính phủ<br></br> 
        /// - DVX: Thị trường phái sinh
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = "m", Order = 2)]
        [DataMember(Name = "ad")]
        public string MarketID { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
        [DataMember(Name = "ae")]
        public long MarketIDSeqNum { get; set; }

        /// <summary>
        /// 2021-03-08 11:14:42 ngocta2
        /// market
        /// <para><i>SPEC version=1.4; date=2020.05.29</i></para>
        /// <para><i>tag=20004; required=Y; format=String; length=2</i></para>
        /// <para><b>ID Bảng giao dịch</b></para>
        /// <para>
        /// G1 : Chính(Main)<br></br>
        /// G2 : Trước giờ giao dịch(Pre Open)<br></br>
        /// G3 : Sau giờ giao dịch(Post Close)<br></br>
        /// G4 : Lô lẻ(Odd lot)<br></br>
        /// G7 : Mua bắt buộc(Buy-in)<br></br>
        /// G8 : Bán bắt buộc(Sell-out)<br></br>
        /// T1 : Thỏa thuận(regular)<br></br>
        /// T4 : Thỏa thuận lô lẻ(regular for Odd lot)<br></br>
        /// T2 : Thỏa thuận trước giờ giao dịch(pre)<br></br>
        /// T3 : Thỏa thuận sau giờ giao dịch(post)<br></br>
        /// T6 : Thỏa thuận sau giờ giao dịch cho lô lẻ(post for Odd lot)<br></br>
        /// R1 : Thỏa thuận(Repo)<br></br>
        /// AL : Tất cả Bảng giao dịch<br></br>
        /// </para>        
        /// </summary>
        [JsonProperty(PropertyName = "b", Order = 2)]
        [DataMember(Name = "af")]
        public string BoardID { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
        [DataMember(Name = "ag")]
        public long BoardIDSeqNum { get; set; }

        /// <summary>
        /// 2020-08-17 15:01:57 ngocta2
        /// reference/ReferencePrice
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20013; required=Y; format=Price; length=15(9.4)</i></para>
        /// <para><b>Giá tham chiếu là mức giá cơ sở để tính giá trần/giá sàn </b></para>
        /// </summary>
        [JsonProperty(PropertyName = "r", Order = 3)]
        [DataMember(Name = "ah")]
        public double ReferencePrice { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "ai")]
        public long ReferencePriceSeqNum { get; set; }

        /// <summary>
        /// 2020-08-17 15:03:09 ngocta2
        /// ceiling/HighLimitPrice
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=1149; required=Y; format=Price; length=15(9.4)  </i></para>
        /// <para><b>Giá trần </b></para>
        /// </summary>
        [JsonProperty(PropertyName = "c", Order = 4)]
        [DataMember(Name = "aj")]
        public double CeilingPrice { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "ak")]
        public long CeilingPriceSeqNum { get; set; }

        /// <summary>
        /// 2020-08-17 15:03:23 ngocta2
        /// floor/LowLimitPrice
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=1148; required=Y; format=Price; length=15(9.4)  </i></para>
        /// <para><b>Giá sàn </b></para>
        /// </summary>
        [JsonProperty(PropertyName = "f", Order = 5)]
        [DataMember(Name = "al")]
        public double FloorPrice { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "am")]
        public long FloorPriceSeqNum { get; set; }

        /// <summary>
        /// 2020-04-10 11:58:50 hungtq
        /// số lệnh mua
        /// BidCount/BuyValidOrderCnt
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30524 ; required=Y; format=Int; length=11</i></para>
        /// <para><b>Số lượng chào giá hợp lệ bên mua</b></para>
        /// </summary>
        [JsonProperty(PropertyName = "bc", Order = 6)]
        [DataMember(Name = "an")]
        public long BidCount { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "ao")]
        public long BidCountSeqNum { get; set; }

        /// <summary>
        /// 2020-04-10 11:58:50 hungtq
        /// tổng KL mua
        /// TotalBidQtty/BuyTotOrderQty
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30522 ; required=Y; format=Int; length=12</i></para>
        /// <para><b>Tổng khối lượng của các lệnh bên mua</b></para>
        /// </summary>
        [JsonProperty(PropertyName = "tbq", Order = 7)]
        [DataMember(Name = "ap")]
        public long TotalBidQtty { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "aq")]
        public long TotalBidQttySeqNum { get; set; }
        // ------------------------------------------------
        [JsonProperty(PropertyName = "bpx", Order = 8)]
        [DataMember(Name = "ar")]
        public double BuyPriceX { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "as")]
        public long BuyPriceXSeqNum { get; set; }

        [JsonProperty(PropertyName = "bqx", Order = 9)]
        [DataMember(Name = "at")]
        public long BuyQuantityX { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "au")]
        public long BuyQuantityXSeqNum { get; set; }

        [JsonProperty(PropertyName = "bp9", Order = 10)]
        [DataMember(Name = "av")]
        public double BuyPrice9 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "aw")]
        public long BuyPrice9SeqNum { get; set; }

        [JsonProperty(PropertyName = "bq9", Order = 11)]
        [DataMember(Name = "ax")]
        public long BuyQuantity9 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "ay")]
        public long BuyQuantity9SeqNum { get; set; }

        [JsonProperty(PropertyName = "bp8", Order = 12)]
        [DataMember(Name = "az")]
        public double BuyPrice8 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "ba")]
        public long BuyPrice8SeqNum { get; set; }

        [JsonProperty(PropertyName = "bq8", Order = 13)]
        [DataMember(Name = "bb")]
        public long BuyQuantity8 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "bc")]
        public long BuyQuantity8SeqNum { get; set; }

        [JsonProperty(PropertyName = "bp7", Order = 14)]
        [DataMember(Name = "bd")]
        public double BuyPrice7 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "be")]
        public long BuyPrice7SeqNum { get; set; }

        [JsonProperty(PropertyName = "bq7", Order = 15)]
        [DataMember(Name = "bf")]
        public long BuyQuantity7 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "bg")]
        public long BuyQuantity7SeqNum { get; set; }

        [JsonProperty(PropertyName = "bp6", Order = 16)]
        [DataMember(Name = "bh")]
        public double BuyPrice6 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "bi")]
        public long BuyPrice6SeqNum { get; set; }

        [JsonProperty(PropertyName = "bq6", Order = 17)]
        [DataMember(Name = "bj")]
        public long BuyQuantity6 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "bk")]
        public long BuyQuantity6SeqNum { get; set; }

        [JsonProperty(PropertyName = "bp5", Order = 18)]
        [DataMember(Name = "bl")]
        public double BuyPrice5 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "bm")]
        public long BuyPrice5SeqNum { get; set; }

        [JsonProperty(PropertyName = "bq5", Order = 19)]
        [DataMember(Name = "bn")]
        public long BuyQuantity5 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "bo")]
        public long BuyQuantity5SeqNum { get; set; }

        [JsonProperty(PropertyName = "bp4", Order = 20)]
        [DataMember(Name = "bp")]
        public double BuyPrice4 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "bq")]
        public long BuyPrice4SeqNum { get; set; }

        [JsonProperty(PropertyName = "bq4", Order = 21)]
        [DataMember(Name = "br")]
        public long BuyQuantity4 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "bs")]
        public long BuyQuantity4SeqNum { get; set; }

        [JsonProperty(PropertyName = "bp3", Order = 22)]
        [DataMember(Name = "bt")]
        public double BuyPrice3 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "bu")]
        public long BuyPrice3SeqNum { get; set; }

        [JsonProperty(PropertyName = "bq3", Order = 23)]
        [DataMember(Name = "bv")]
        public long BuyQuantity3 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "bw")]
        public long BuyQuantity3SeqNum { get; set; }

        [JsonProperty(PropertyName = "bp2", Order = 24)]
        [DataMember(Name = "bx")]
        public double BuyPrice2 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "by")]
        public long BuyPrice2SeqNum { get; set; }

        [JsonProperty(PropertyName = "bq2", Order = 25)]
        [DataMember(Name = "bz")]
        public long BuyQuantity2 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "ca")]
        public long BuyQuantity2SeqNum { get; set; }

        [JsonProperty(PropertyName = "bp1", Order = 26)]
        [DataMember(Name = "cb")]
        public double BuyPrice1 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "cc")]
        public long BuyPrice1SeqNum { get; set; }

        [JsonProperty(PropertyName = "bq1", Order = 27)]
        [DataMember(Name = "cd")]
        public long BuyQuantity1 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "ce")]
        public long BuyQuantity1SeqNum { get; set; }
        // ------------------------------------------------
        [JsonProperty(PropertyName = "mp", Order = 28)]
        [DataMember(Name = "cf")]
        public double MatchPrice { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "cg")]
        public long MatchPriceSeqNum { get; set; }

        [JsonProperty(PropertyName = "mq", Order = 29)]
        [DataMember(Name = "ch")]
        public long MatchQuantity { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "ci")]
        public long MatchQuantitySeqNum { get; set; }

        [JsonProperty(PropertyName = "mc", Order = 30)]
        [DataMember(Name = "cj")]
        public double MatchChange { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "ck")]
        public long MatchChangeSeqNum { get; set; }

        [JsonProperty(PropertyName = "mcp", Order = 31)]
        [DataMember(Name = "cl")]
        public double MatchChangePercent { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "cm")]
        public long MatchChangePercentSeqNum { get; set; }

        [JsonProperty(PropertyName = "tnq", Order = 32)]
        [DataMember(Name = "cn")]
        public long TotalNMQuantity { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "co")]
        public long TotalNMQuantitySeqNum { get; set; }
        // ------------------------------------------------
        [JsonProperty(PropertyName = "sp1", Order = 33)]
        [DataMember(Name = "cp")]
        public double SellPrice1 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "cq")]
        public long SellPrice1SeqNum { get; set; }

        [JsonProperty(PropertyName = "sq1", Order = 34)]
        [DataMember(Name = "cr")]
        public long SellQuantity1 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "cs")]
        public long SellQuantity1SeqNum { get; set; }

        [JsonProperty(PropertyName = "sp2", Order = 35)]
        [DataMember(Name = "ct")]
        public double SellPrice2 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "cu")]
        public long SellPrice2SeqNum { get; set; }

        [JsonProperty(PropertyName = "sq2", Order = 36)]
        [DataMember(Name = "cv")]
        public long SellQuantity2 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "cw")]
        public long SellQuantity2SeqNum { get; set; }

        [JsonProperty(PropertyName = "sp3", Order = 37)]
        [DataMember(Name = "cx")]
        public double SellPrice3 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "cy")]
        public long SellPrice3SeqNum { get; set; }

        [JsonProperty(PropertyName = "sq3", Order = 38)]
        [DataMember(Name = "cz")]
        public long SellQuantity3 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "da")]
        public long SellQuantity3SeqNum { get; set; }

        [JsonProperty(PropertyName = "sp4", Order = 39)]
        [DataMember(Name = "db")]
        public double SellPrice4 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "dc")]
        public long SellPrice4SeqNum { get; set; }

        [JsonProperty(PropertyName = "sq4", Order = 40)]
        [DataMember(Name = "dd")]
        public long SellQuantity4 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "de")]
        public long SellQuantity4SeqNum { get; set; }

        [JsonProperty(PropertyName = "sp5", Order = 41)]
        [DataMember(Name = "df")]
        public double SellPrice5 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "dg")]
        public long SellPrice5SeqNum { get; set; }

        [JsonProperty(PropertyName = "sq5", Order = 42)]
        [DataMember(Name = "dh")]
        public long SellQuantity5 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "di")]
        public long SellQuantity5SeqNum { get; set; }

        [JsonProperty(PropertyName = "sp6", Order = 43)]
        [DataMember(Name = "dj")]
        public double SellPrice6 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "dk")]
        public long SellPrice6SeqNum { get; set; }

        [JsonProperty(PropertyName = "sq6", Order = 44)]
        [DataMember(Name = "dl")]
        public long SellQuantity6 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "dm")]
        public long SellQuantity6SeqNum { get; set; }

        [JsonProperty(PropertyName = "sp7", Order = 45)]
        [DataMember(Name = "dn")]
        public double SellPrice7 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "do")]
        public long SellPrice7SeqNum { get; set; }

        [JsonProperty(PropertyName = "sq7", Order = 46)]
        [DataMember(Name = "dp")]
        public long SellQuantity7 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "dq")]
        public long SellQuantity7SeqNum { get; set; }

        [JsonProperty(PropertyName = "sp8", Order = 47)]
        [DataMember(Name = "dr")]
        public double SellPrice8 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "ds")]
        public long SellPrice8SeqNum { get; set; }

        [JsonProperty(PropertyName = "sq8", Order = 48)]
        [DataMember(Name = "dt")]
        public long SellQuantity8 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "du")]
        public long SellQuantity8SeqNum { get; set; }

        [JsonProperty(PropertyName = "sp9", Order = 49)]
        [DataMember(Name = "dv")]
        public double SellPrice9 { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "dw")]
        public long SellPrice9SeqNum { get; set; }

        [JsonProperty(PropertyName = "sq9", Order = 50)]
        [DataMember(Name = "dx")]
        public long SellQuantity9 { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "dy")]
        public long SellQuantity9SeqNum { get; set; }

        [JsonProperty(PropertyName = "spx", Order = 51)]
        [DataMember(Name = "dz")]
        public double SellPriceX { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "ea")]
        public long SellPriceXSeqNum { get; set; }

        [JsonProperty(PropertyName = "sqx", Order = 52)]
        [DataMember(Name = "eb")]
        public long SellQuantityX { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "ec")]
        public long SellQuantityXSeqNum { get; set; }
        // ------------------------------------------------

        /// <summary>
        /// 2020-04-10 11:58:50 hungtq
        /// số lệnh bán
        /// OfferCount - X.SellValidOrderCnt.30523
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30524 ; required=Y; format=Int; length=11</i></para>
        /// <para><b>Số lượng chào giá hợp lệ bên mua</b></para>
        /// </summary>
        [JsonProperty(PropertyName = "oc", Order = 53)]
        [DataMember(Name = "ed")]
        public long OfferCount { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "ee")]
        public long OfferCountSeqNum { get; set; }

        /// <summary>
        /// 2020-04-10 11:58:50 hungtq
        /// tổng KL bán
        /// TotalOfferQtty - X.SellTotOrderQty.30521
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30522 ; required=Y; format=Int; length=12</i></para>
        /// <para><b>Tổng khối lượng của các lệnh bên mua</b></para>
        /// </summary>
        [JsonProperty(PropertyName = "toq", Order = 54)]
        [DataMember(Name = "ef")]
        public long TotalOfferQtty { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "eg")]
        public long TotalOfferQttySeqNum { get; set; }

        /// <summary>
        /// X	MDEntryPx	270	124051.0	VN0HCMA18059-HCMA1805	X.269=4
        /// </summary>
        [JsonProperty(PropertyName = "op", Order = 55)]
        [DataMember(Name = "eh")]
        public double OpenPrice { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "ei")]
        public long OpenPriceSeqNum { get; set; }

        /// <summary>
        /// d	VWAP	30625
        /// </summary>
        [JsonProperty(PropertyName = "av", Order = 56)]
        [DataMember(Name = "ej")]
        public double AveragePrice { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "ek")]
        public long AveragePriceSeqNum { get; set; }

        /// <summary>
        /// X	MDEntryPx	270	124051.0	VN0HCMA18059-HCMA1805	X.269=7
        /// </summary>
        [JsonProperty(PropertyName = "hi", Order = 57)]
        [DataMember(Name = "el")]
        public double HighestPrice { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "em")]
        public long HighestPriceSeqNum { get; set; }

        /// <summary>
        /// X	MDEntryPx	270	124051.0	VN0HCMA18059-HCMA1805	X.269=8
        /// </summary>
        [JsonProperty(PropertyName = "lo", Order = 58)]
        [DataMember(Name = "en")]
        public double LowestPrice { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "eo")]
        public long LowestPriceSeqNum { get; set; }

        /// <summary>
        /// KL nước ngoài mua
        /// MT	BuyVolume	330	1750	VN2STOST0106-2STOST01
        /// </summary>
        [JsonProperty(PropertyName = "fbq", Order = 59)]
        [DataMember(Name = "ep")]
        public long ForeignBuyQuantity { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "eq")]
        public long ForeignBuyQuantitySeqNum { get; set; }

        /// <summary>
        /// KL nước ngoài bán
        /// MT	SellVolume	331	310	VN2STOST0106-2STOST01
        /// </summary>
        [JsonProperty(PropertyName = "fsq", Order = 60)]
        [DataMember(Name = "er")]
        public long ForeignSellQuantity { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "es")]
        public long ForeignSellQuantitySeqNum { get; set; }

        /// <summary>
        /// KL còn lại được phép mua
        /// MF	ForeignerOrderLimitQty	30558	600000	VN6068921660-B06892166
        /// </summary>
        [JsonProperty(PropertyName = "frr", Order = 61)]
        [DataMember(Name = "et")]
        public long ForeignRoomRemain { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "eu")]
        public long ForeignRoomRemainSeqNum { get; set; }

        /// <summary>
        /// KL mở
        /// d	OpenInterestQty	30540
        /// </summary>
        [JsonProperty(PropertyName = "oi", Order = 62)]
        [DataMember(Name = "ev")]
        public long OpenInterest { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "ew")]
        public long OpenInterestSeqNum { get; set; }

        /// <summary>
        /// ngày đáo hạn
        /// d	FinalTradeDate	30511   (30511=20191216 => 16/12/2019)
        /// </summary>
        [JsonProperty(PropertyName = "ltd", Order = 63)]
        [DataMember(Name = "ex")]
        public string LastTradingDate { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
        [DataMember(Name = "ey")]
        public long LastTradingDateSeqNum { get; set; }

        /// <summary>
        /// 2019-12-09 15:51:26 hungtq
        /// Giao dịch không hưởng quyền
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20018  ; required=N; format=String; length=2 </i></para>
        /// <para><b>Giao dịch không hưởng quyền </b></para>
        /// <para>
        ///00: Không có <br></br>
        ///01: Chia cổ phiếu/Cổ phiếu thưởng <br></br>
        ///02: Chia cổ tức tiền mặt. <br></br>
        ///04: Phát hành quyền <br></br>
        ///03: Chia cổ phiếu/Cổ phiếu thưởng + Chia cổ tức tiền mặt. <br></br>
        ///05: Phát hành quyền + Chia cổ phiếu/Cổ phiếu thưởng <br></br>
        ///06:  Phát hành quyền + Chia cổ tức tiền mặt <br></br>
        ///07:  Phát hành quyền + Chia cổ phiếu/Cổ phiếu thưởng + Chia cổ tức <br></br>
        ///08:  Trái phiếu chuyển đổi <br></br>
        ///16: Chia cổ phiếu quỹ. <br></br>
        ///P1: Tách cổ phiếu <br></br>
        ///P2: Gộp cổ phiếu <br></br>
        ///XX: V.v. <br></br>
        ///</para>
        /// </summary>
        [JsonProperty(PropertyName = "ect", Order = 64)]
        [DataMember(Name = "ez")]
        public string ExClassType { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
        [DataMember(Name = "fa")]
        public long ExClassTypeSeqNum { get; set; }

        /// <summary>
        /// 2019-12-09 15:52:56 hungtq
        /// Ký hiệu để xác định loại giao dịch cho một mã chứng khoán.
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30636    ; required=N; format=String; length=3</i></para>
        /// <para><b> Ký hiệu để xác định loại giao dịch cho một mã chứng khoán.</b></para>
        /// <para>
        ///NRM: Bình thường <br></br>
        ///SNE: Cơ chế giao dịch đặc biệt đối với chứng khoán không có giao dịch trong khoảng thời gian quy định  <br></br>
        ///SLS: Cơ chế giao dịch đặc biệt đối với chứng khoán sau khi bị tạm ngưng giao dịch trong khoảng thời gian quy định. <br></br>
        ///NWN: Niêm yết mới với biên độ giá thông thường <br></br>
        ///NWE: Niêm yết mới với biên độ giá đặc biệt <br></br>
        ///</para>
        /// </summary>
        [JsonProperty(PropertyName = "stm", Order = 65)]
        [DataMember(Name = "fb")]
        public string SymbolTradingMethod { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
        [DataMember(Name = "fc")]
        public long SymbolTradingMethodSeqNum { get; set; }

        /// <summary>
        /// 2019-12-09 15:53:45 hungtq
        /// Ký hiệu để phân loại tình trạng giao dịch như tạm ngưng, ngưng giao dịch vì một số lý do.
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30637     ; required=N; format=String; length=3</i></para>
        /// <para><b>Ký hiệu để phân loại tình trạng giao dịch như tạm ngưng, ngưng giao dịch vì một số lý do. </b></para>
        /// <para>
        ///NRM: Bình thường <br></br>
        ///DTL: Hủy niêm yết để chuyển sàn <br></br>
        ///SUS: Tạm ngưng giao dịch <br></br>
        ///TFR: Ngưng giao dịch do bị hạn chế. <br></br>
        ///</para>
        /// </summary>
        [JsonProperty(PropertyName = "sts", Order = 66)]
        [DataMember(Name = "fd")]
        public string SymbolTradingSantion { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
        [DataMember(Name = "fe")]
        public long SymbolTradingSantionSeqNum { get; set; }

        /// <summary>
        /// 2020-04-27 14:59:22 hungtq
        /// "Giá dự kiến được thực hiện trong phiên khớp lệnh định kỳ khi RE         
        /// được áp dụng(Sự kiện RE xảy ra trong các đợt giao dịch khớp lệnh định kỳ.)"
        /// MW	RandomEndTentativeExecutionPrice	30616
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30616; required=Y; format=Price; length=15(9.4)</i></para>
        /// <para><b>Giá dự kiến được thực hiện trong phiên khớp lệnh định kỳ khi RE được áp dụng</b></para>
        /// khi Serialize thành string thì không có field này
        /// </summary>
        [JsonProperty(PropertyName = "tep", Order = 67)]
        [DataMember(Name = "ff")]
        public double TentativeExecutionPrice { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "fg")]
        public long TentativeExecutionPriceSeqNum { get; set; }

        /// <summary>
        /// 2020-04-27 14:55:20 hungtq
        /// Giá dự kiến giao dịch (phiên định kỳ khớp lệnh mở cửa)
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30552 ; required=Y; format=Price; length=15(9.4)</i></para>
        /// <para><b>Giá dự kiến </b></para>
        /// khi Serialize thành string thì không có field này
        /// </summary>
        [JsonProperty(PropertyName = "etp", Order = 68)]
        [DataMember(Name = "fh")]
        public double ExpectedTradePx { get; set; } = EGlobalConfig.__INIT_NULL_DOUBLE;
        [DataMember(Name = "fi")]
        public long ExpectedTradePxSeqNum { get; set; }

        /// <summary>
        /// 2020-04-27 14:55:20 hungtq
        /// Khối lượng dự kiến giao dịch (phiên định kỳ khớp lệnh mở cửa)
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=30553 ; required=Y; format=Int; length=12</i></para>
        /// <para><b>Khối lượng dự kiến giao dịch</b></para>
        /// khi Serialize thành string thì không có field này
        /// </summary>
        [JsonProperty(PropertyName = "etq", Order = 69)]
        [DataMember(Name = "fj")]
        public long ExpectedTradeQty { get; set; } = EGlobalConfig.__INIT_NULL_LONG;
        [DataMember(Name = "fk")]
        public long ExpectedTradeQtySeqNum { get; set; }

        /// <summary>
        /// 2019-11-28 16:04:10 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20005; required=Y; format=String; length=3</i></para>
        /// <para><b>Xác định hoạt động bảng giao dịch. Về cơ bản, lịch trình giao dịch là một loạt các hoạt động sự kiện được xác định bẳng một mã 3 ký tự, theo đây là ví dụ:</b></para>
        /// <para>
        /// 0915: Opening call auction (AA1) <br></br>
        /// 0930: Continuous trading (BB1) <br></br>
        /// 1430: Closing call auction (BC1) <br></br>
        /// 1445: Market close (AC2) <br></br>
        /// AA1 : Opening CA Open <br></br>
        /// AB1 : Cont.A Open <br></br>
        /// AB2 : Board Trading Close <br></br>
        /// AC2 : Closing CA Execution <br></br>
        /// AD2 : CA Excecution <br></br>
        /// AE1 : Market Halt Release <br></br>
        /// AE8 : Market Halt <br></br>
        /// AF1 : CA Open after CB <br></br>
        /// AF8 : Suspension by CB <br></br>
        /// AW8 : Lunch Break <br></br>
        /// AW9 : Open after Lunch Break <br></br>
        /// BB1 : Cont.A Open for Non-PCA Instr. <br></br>
        /// BC1 : Closing CA Open <br></br>
        /// BE9 : Cont.A Open after Market Halt <br></br>
        /// BF9 : Cont.A Open after CB <br></br>
        /// CC1 : Closing PCA Open <br></br>
        /// CD1 : PCA Open <br></br>
        /// DB2 : Trading Close for LTD Instr. <br></br>
        /// DC2 : Closing CA Execution for LTD Instr. <br></br>
        /// EI1 : Trading Halt Release by Instr./Product <br></br>
        /// EI8 : Instr./Product Halt <br></br>
        /// EI9 : Cont.A Open after Instr./Product Halt
        /// </para>
        /// </summary>        
        [JsonProperty(PropertyName = "be", Order = 70)]
        [DataMember(Name = "fl")]
        public string BoardEvtID { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
        [DataMember(Name = "fm")]
        public long BoardEvtIDSeqNum { get; set; }

        /// <summary>
        /// 2019-12-10 14:30:10 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=20008 ; required=N; format=String; length=2</i></para>
        /// <para><b>Mã bắt đầu/kết thúc đợt giao dịch/ Bảng giao dịch </b></para>
        /// <para>
        ///BS : Bắt đầu Bảng giao dịch<br></br>
        ///BE : Kết thúc Bảng giao dịch<br></br>
        ///SS : Bắt đầu Đợt giao dịch <br></br>
        ///SE : Kết thúc Đợt giao dịch <br></br>
        ///SH : Tạm dừng Đợt giao dịch <br></br>
        ///SR : Bắt đầu lại Đợt giao dịch<br></br>
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = "sc", Order = 71)]
        [DataMember(Name = "fn")]
        public string SessOpenCloseCode { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
        [DataMember(Name = "fo")]
        public long SessOpenCloseCodeSeqNum { get; set; }

        /// <summary>
        /// 2019-12-10 14:31:10 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=336 ; required=Y; format=String; length=2</i></para>
        /// <para>
        ///01 = Nạp lại Lệnh GT <br></br>
        ///10 = Phiên mở cửa <br></br>
        ///11 = Phiên mở cửa (mở rộng) <br></br>
        ///20 = Phiên định kỳ sau khi dừng thị trường <br></br>
        ///21 = Phiên định kỳ sau khi dừng thị trường (mở rộng) <br></br>
        ///30 = Kết thúc phiên định kỳ <br></br>
        ///40 = Phiên liên tục <br></br>
        ///50 = Kiểm soát biến động giá <br></br>
        ///51 = Kiểm soát biến động giá (mở rộng) <br></br>
        ///60 = Tiếp nhận giá đóng cửa sau khi đóng cửa <br></br>
        ///80 = Phiên khớp lệnh định kỳ nhiều lần <br></br>
        ///90 = Tạm ngừng giao dịch <br></br>
        ///99 = Đóng cửa thị trường <br></br>
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = "ts", Order = 72)]
        [DataMember(Name = "fp")]
        public string TradingSessionID { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
        [DataMember(Name = "fq")]
        public long TradingSessionIDSeqNum { get; set; }

        /// <summary>
        /// 2021-10-21 10:39:45 ngocta2
        /// <para><i>SPEC version=1.4; date=29.05.2020</i></para>
        /// <para><i>tag=60 ; required=Y; format=UTCTime; length=9</i></para>
        /// <para>        
        /// Thời gian thực thi HHmmSSsss(TAChartSaver input)
        /// </para>
        /// </summary>
        [JsonProperty(PropertyName = "tt", Order = 73)]
        [DataMember(Name = "fr")]
        public string TransactTime { get; set; } = EGlobalConfig.__INIT_NULL_STRING;
        [DataMember(Name = "fs")]
        public long TransactTimeSeqNum { get; set; }

        /// <summary>
        /// 2021-10-22 14:04:17 ngocta2
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=75 ; required=Y; format=LocalMkt Date; length=8</i></para>
        /// <para><b>Ngày giao dịch. Định dạng (YYYYMMDD)</b></para>
        /// </summary>
        [DataMember(Name = "td")]
        [JsonProperty(PropertyName = "td", Order = 74)]
        public string TradeDate { get; set; }
        [DataMember(Name = "te")]
        public long TradeDateSeqNum { get; set; }

        // ----------------------------------------------------------------------------------
        public EXQuote() { }
        public EXQuote(long seqNum)
        {
            this.MarketIDSeqNum           = seqNum;
            this.BoardIDSeqNum            = seqNum;
            //this.ISINSeqNum               = seqNum;
            this.SymbolSeqNum             = seqNum;
            this.ReferencePriceSeqNum     = seqNum; // 10:30 AM 8/13/2021 ngocta2
            this.CeilingPriceSeqNum       = seqNum; // 10:30 AM 8/13/2021 ngocta2
            this.FloorPriceSeqNum         = seqNum; // 10:30 AM 8/13/2021 ngocta2
            this.BidCountSeqNum           = seqNum;
            this.TotalBidQttySeqNum       = seqNum;
            this.BuyPriceXSeqNum          = seqNum;
            this.BuyQuantityXSeqNum       = seqNum;
            this.BuyPrice9SeqNum          = seqNum;
            this.BuyQuantity9SeqNum       = seqNum;
            this.BuyPrice8SeqNum          = seqNum;
            this.BuyQuantity8SeqNum       = seqNum;
            this.BuyPrice7SeqNum          = seqNum;
            this.BuyQuantity7SeqNum       = seqNum;
            this.BuyPrice6SeqNum          = seqNum;
            this.BuyQuantity6SeqNum       = seqNum;
            this.BuyPrice5SeqNum          = seqNum;
            this.BuyQuantity5SeqNum       = seqNum;
            this.BuyPrice4SeqNum          = seqNum;
            this.BuyQuantity4SeqNum       = seqNum;
            this.BuyPrice3SeqNum          = seqNum;
            this.BuyQuantity3SeqNum       = seqNum;
            this.BuyPrice2SeqNum          = seqNum;
            this.BuyQuantity2SeqNum       = seqNum;
            this.BuyPrice1SeqNum          = seqNum;
            this.BuyQuantity1SeqNum       = seqNum;
            this.MatchPriceSeqNum         = seqNum;
            this.MatchQuantitySeqNum      = seqNum;
            this.MatchChangeSeqNum        = seqNum;
            this.MatchChangePercentSeqNum = seqNum;
            this.TotalNMQuantitySeqNum    = seqNum;
            this.SellPrice1SeqNum         = seqNum;
            this.SellQuantity1SeqNum      = seqNum;
            this.SellPrice2SeqNum         = seqNum;
            this.SellQuantity2SeqNum      = seqNum;
            this.SellPrice3SeqNum         = seqNum;
            this.SellQuantity3SeqNum      = seqNum;
            this.SellPrice4SeqNum         = seqNum;
            this.SellQuantity4SeqNum      = seqNum;
            this.SellPrice5SeqNum         = seqNum;
            this.SellQuantity5SeqNum      = seqNum;
            this.SellPrice6SeqNum         = seqNum;
            this.SellQuantity6SeqNum      = seqNum;
            this.SellPrice7SeqNum         = seqNum;
            this.SellQuantity7SeqNum      = seqNum;
            this.SellPrice8SeqNum         = seqNum;
            this.SellQuantity8SeqNum      = seqNum;
            this.SellPrice9SeqNum         = seqNum;
            this.SellQuantity9SeqNum      = seqNum;
            this.SellPriceXSeqNum         = seqNum;
            this.SellQuantityXSeqNum      = seqNum;
            this.OfferCountSeqNum         = seqNum;
            this.TotalOfferQttySeqNum     = seqNum;
            this.OpenPriceSeqNum          = seqNum;
            this.HighestPriceSeqNum       = seqNum;
            this.LowestPriceSeqNum        = seqNum;
            //--------------
            this.ForeignBuyQuantitySeqNum      = seqNum;
            this.ForeignSellQuantitySeqNum     = seqNum;
            this.ForeignRoomRemainSeqNum       = seqNum;
            this.OpenInterestSeqNum            = seqNum;
            this.LastTradingDateSeqNum         = seqNum;
            this.ExClassTypeSeqNum             = seqNum;
            this.SymbolTradingMethodSeqNum     = seqNum;
            this.SymbolTradingSantionSeqNum    = seqNum;
            this.TentativeExecutionPriceSeqNum = seqNum;
            this.ExpectedTradePxSeqNum         = seqNum;
            this.ExpectedTradeQtySeqNum        = seqNum;
            this.BoardEvtIDSeqNum              = seqNum;
            this.SessOpenCloseCodeSeqNum       = seqNum;
            this.TradingSessionIDSeqNum        = seqNum;
            this.TransactTimeSeqNum            = seqNum;
            this.TradeDateSeqNum               = seqNum;
        }

        /// <summary>
        /// lan dau tien tao data full cua ma, init tat ca = 0
        /// khong the default value =-1 duoc
        /// </summary>
        /// <param name="xQuote"></param>
        /// <returns></returns>
        public EXQuote InitZero(ref EXQuote xQuote)
        {
            if (xQuote.ISIN                    == EGlobalConfig.__INIT_NULL_STRING) this.ISIN                    = EGlobalConfig.__STRING_BLANK; else this.ISIN                    = xQuote.ISIN; 
            if (xQuote.Symbol                  == EGlobalConfig.__INIT_NULL_STRING) this.Symbol                  = EGlobalConfig.__STRING_BLANK; else this.Symbol                  = xQuote.Symbol; 
            if (xQuote.MarketID                == EGlobalConfig.__INIT_NULL_STRING) this.MarketID                = EGlobalConfig.__STRING_BLANK; else this.MarketID                = xQuote.MarketID; 
            if (xQuote.BoardID                 == EGlobalConfig.__INIT_NULL_STRING) this.BoardID                 = EGlobalConfig.__STRING_BLANK; else this.BoardID                 = xQuote.BoardID; 
            if (xQuote.ReferencePrice          == EGlobalConfig.__INIT_NULL_DOUBLE) this.ReferencePrice          = EGlobalConfig.__LONG_0;       else this.ReferencePrice          = xQuote.ReferencePrice; 
            if (xQuote.CeilingPrice            == EGlobalConfig.__INIT_NULL_DOUBLE) this.CeilingPrice            = EGlobalConfig.__LONG_0;       else this.CeilingPrice            = xQuote.CeilingPrice; 
            if (xQuote.FloorPrice              == EGlobalConfig.__INIT_NULL_DOUBLE) this.FloorPrice              = EGlobalConfig.__LONG_0;       else this.FloorPrice              = xQuote.FloorPrice; 
            if (xQuote.BidCount                == EGlobalConfig.__INIT_NULL_LONG)   this.BidCount                = EGlobalConfig.__LONG_0;       else this.BidCount                = xQuote.BidCount; 
            if (xQuote.TotalBidQtty            == EGlobalConfig.__INIT_NULL_LONG)   this.TotalBidQtty            = EGlobalConfig.__LONG_0;       else this.TotalBidQtty            = xQuote.TotalBidQtty; 
            if (xQuote.BuyPriceX               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPriceX               = EGlobalConfig.__LONG_0;       else this.BuyPriceX               = xQuote.BuyPriceX; 
            if (xQuote.BuyQuantityX            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantityX            = EGlobalConfig.__LONG_0;       else this.BuyQuantityX            = xQuote.BuyQuantityX; 
            if (xQuote.BuyPrice9               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice9               = EGlobalConfig.__LONG_0;       else this.BuyPrice9               = xQuote.BuyPrice9; 
            if (xQuote.BuyQuantity9            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity9            = EGlobalConfig.__LONG_0;       else this.BuyQuantity9            = xQuote.BuyQuantity9; 
            if (xQuote.BuyPrice8               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice8               = EGlobalConfig.__LONG_0;       else this.BuyPrice8               = xQuote.BuyPrice8; 
            if (xQuote.BuyQuantity8            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity8            = EGlobalConfig.__LONG_0;       else this.BuyQuantity8            = xQuote.BuyQuantity8; 
            if (xQuote.BuyPrice7               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice7               = EGlobalConfig.__LONG_0;       else this.BuyPrice7               = xQuote.BuyPrice7; 
            if (xQuote.BuyQuantity7            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity7            = EGlobalConfig.__LONG_0;       else this.BuyQuantity7            = xQuote.BuyQuantity7; 
            if (xQuote.BuyPrice6               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice6               = EGlobalConfig.__LONG_0;       else this.BuyPrice6               = xQuote.BuyPrice6; 
            if (xQuote.BuyQuantity6            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity6            = EGlobalConfig.__LONG_0;       else this.BuyQuantity6            = xQuote.BuyQuantity6; 
            if (xQuote.BuyPrice5               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice5               = EGlobalConfig.__LONG_0;       else this.BuyPrice5               = xQuote.BuyPrice5; 
            if (xQuote.BuyQuantity5            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity5            = EGlobalConfig.__LONG_0;       else this.BuyQuantity5            = xQuote.BuyQuantity5; 
            if (xQuote.BuyPrice4               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice4               = EGlobalConfig.__LONG_0;       else this.BuyPrice4               = xQuote.BuyPrice4; 
            if (xQuote.BuyQuantity4            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity4            = EGlobalConfig.__LONG_0;       else this.BuyQuantity4            = xQuote.BuyQuantity4; 
            if (xQuote.BuyPrice3               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice3               = EGlobalConfig.__LONG_0;       else this.BuyPrice3               = xQuote.BuyPrice3; 
            if (xQuote.BuyQuantity3            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity3            = EGlobalConfig.__LONG_0;       else this.BuyQuantity3            = xQuote.BuyQuantity3; 
            if (xQuote.BuyPrice2               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice2               = EGlobalConfig.__LONG_0;       else this.BuyPrice2               = xQuote.BuyPrice2; 
            if (xQuote.BuyQuantity2            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity2            = EGlobalConfig.__LONG_0;       else this.BuyQuantity2            = xQuote.BuyQuantity2; 
            if (xQuote.BuyPrice1               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice1               = EGlobalConfig.__LONG_0;       else this.BuyPrice1               = xQuote.BuyPrice1; 
            if (xQuote.BuyQuantity1            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity1            = EGlobalConfig.__LONG_0;       else this.BuyQuantity1            = xQuote.BuyQuantity1; 
            if (xQuote.MatchPrice              == EGlobalConfig.__INIT_NULL_DOUBLE) this.MatchPrice              = EGlobalConfig.__LONG_0;       else this.MatchPrice              = xQuote.MatchPrice; 
            if (xQuote.MatchQuantity           == EGlobalConfig.__INIT_NULL_LONG)   this.MatchQuantity           = EGlobalConfig.__LONG_0;       else this.MatchQuantity           = xQuote.MatchQuantity; 
            if (xQuote.MatchChange             == EGlobalConfig.__INIT_NULL_DOUBLE) this.MatchChange             = EGlobalConfig.__LONG_0;       else this.MatchChange             = xQuote.MatchChange; 
            if (xQuote.MatchChangePercent      == EGlobalConfig.__INIT_NULL_DOUBLE) this.MatchChangePercent      = EGlobalConfig.__LONG_0;       else this.MatchChangePercent      = xQuote.MatchChangePercent; 
            if (xQuote.TotalNMQuantity         == EGlobalConfig.__INIT_NULL_LONG)   this.TotalNMQuantity         = EGlobalConfig.__LONG_0;       else this.TotalNMQuantity         = xQuote.TotalNMQuantity; 
            if (xQuote.SellPrice1              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice1              = EGlobalConfig.__LONG_0;       else this.SellPrice1              = xQuote.SellPrice1; 
            if (xQuote.SellQuantity1           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity1           = EGlobalConfig.__LONG_0;       else this.SellQuantity1           = xQuote.SellQuantity1; 
            if (xQuote.SellPrice2              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice2              = EGlobalConfig.__LONG_0;       else this.SellPrice2              = xQuote.SellPrice2; 
            if (xQuote.SellQuantity2           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity2           = EGlobalConfig.__LONG_0;       else this.SellQuantity2           = xQuote.SellQuantity2; 
            if (xQuote.SellPrice3              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice3              = EGlobalConfig.__LONG_0;       else this.SellPrice3              = xQuote.SellPrice3; 
            if (xQuote.SellQuantity3           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity3           = EGlobalConfig.__LONG_0;       else this.SellQuantity3           = xQuote.SellQuantity3; 
            if (xQuote.SellPrice4              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice4              = EGlobalConfig.__LONG_0;       else this.SellPrice4              = xQuote.SellPrice4; 
            if (xQuote.SellQuantity4           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity4           = EGlobalConfig.__LONG_0;       else this.SellQuantity4           = xQuote.SellQuantity4; 
            if (xQuote.SellPrice5              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice5              = EGlobalConfig.__LONG_0;       else this.SellPrice5              = xQuote.SellPrice5; 
            if (xQuote.SellQuantity5           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity5           = EGlobalConfig.__LONG_0;       else this.SellQuantity5           = xQuote.SellQuantity5; 
            if (xQuote.SellPrice6              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice6              = EGlobalConfig.__LONG_0;       else this.SellPrice6              = xQuote.SellPrice6; 
            if (xQuote.SellQuantity6           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity6           = EGlobalConfig.__LONG_0;       else this.SellQuantity6           = xQuote.SellQuantity6; 
            if (xQuote.SellPrice7              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice7              = EGlobalConfig.__LONG_0;       else this.SellPrice7              = xQuote.SellPrice7; 
            if (xQuote.SellQuantity7           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity7           = EGlobalConfig.__LONG_0;       else this.SellQuantity7           = xQuote.SellQuantity7; 
            if (xQuote.SellPrice8              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice8              = EGlobalConfig.__LONG_0;       else this.SellPrice8              = xQuote.SellPrice8; 
            if (xQuote.SellQuantity8           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity8           = EGlobalConfig.__LONG_0;       else this.SellQuantity8           = xQuote.SellQuantity8; 
            if (xQuote.SellPrice9              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice9              = EGlobalConfig.__LONG_0;       else this.SellPrice9              = xQuote.SellPrice9; 
            if (xQuote.SellQuantity9           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity9           = EGlobalConfig.__LONG_0;       else this.SellQuantity9           = xQuote.SellQuantity9; 
            if (xQuote.SellPriceX              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPriceX              = EGlobalConfig.__LONG_0;       else this.SellPriceX              = xQuote.SellPriceX; 
            if (xQuote.SellQuantityX           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantityX           = EGlobalConfig.__LONG_0;       else this.SellQuantityX           = xQuote.SellQuantityX; 
            if (xQuote.OfferCount              == EGlobalConfig.__INIT_NULL_LONG)   this.OfferCount              = EGlobalConfig.__LONG_0;       else this.OfferCount              = xQuote.OfferCount; 
            if (xQuote.TotalOfferQtty          == EGlobalConfig.__INIT_NULL_LONG)   this.TotalOfferQtty          = EGlobalConfig.__LONG_0;       else this.TotalOfferQtty          = xQuote.TotalOfferQtty; 
            if (xQuote.OpenPrice               == EGlobalConfig.__INIT_NULL_DOUBLE) this.OpenPrice               = EGlobalConfig.__LONG_0;       else this.OpenPrice               = xQuote.OpenPrice; 
            if (xQuote.AveragePrice            == EGlobalConfig.__INIT_NULL_DOUBLE) this.AveragePrice            = EGlobalConfig.__LONG_0;       else this.AveragePrice            = xQuote.AveragePrice; 
            if (xQuote.HighestPrice            == EGlobalConfig.__INIT_NULL_DOUBLE) this.HighestPrice            = EGlobalConfig.__LONG_0;       else this.HighestPrice            = xQuote.HighestPrice; 
            if (xQuote.LowestPrice             == EGlobalConfig.__INIT_NULL_DOUBLE) this.LowestPrice             = EGlobalConfig.__LONG_0;       else this.LowestPrice             = xQuote.LowestPrice; 
            if (xQuote.ForeignBuyQuantity      == EGlobalConfig.__INIT_NULL_LONG)   this.ForeignBuyQuantity      = EGlobalConfig.__LONG_0;       else this.ForeignBuyQuantity      = xQuote.ForeignBuyQuantity; 
            if (xQuote.ForeignSellQuantity     == EGlobalConfig.__INIT_NULL_LONG)   this.ForeignSellQuantity     = EGlobalConfig.__LONG_0;       else this.ForeignSellQuantity     = xQuote.ForeignSellQuantity; 
            if (xQuote.ForeignRoomRemain       == EGlobalConfig.__INIT_NULL_DOUBLE) this.ForeignRoomRemain       = EGlobalConfig.__LONG_0;       else this.ForeignRoomRemain       = xQuote.ForeignRoomRemain; 
            if (xQuote.OpenInterest            == EGlobalConfig.__INIT_NULL_DOUBLE) this.OpenInterest            = EGlobalConfig.__LONG_0;       else this.OpenInterest            = xQuote.OpenInterest; 
            if (xQuote.LastTradingDate         == EGlobalConfig.__INIT_NULL_STRING) this.LastTradingDate         = EGlobalConfig.__STRING_BLANK; else this.LastTradingDate         = xQuote.LastTradingDate; 
            if (xQuote.ExClassType             == EGlobalConfig.__INIT_NULL_STRING) this.ExClassType             = EGlobalConfig.__STRING_BLANK; else this.ExClassType             = xQuote.ExClassType; 
            if (xQuote.SymbolTradingMethod     == EGlobalConfig.__INIT_NULL_STRING) this.SymbolTradingMethod     = EGlobalConfig.__STRING_BLANK; else this.SymbolTradingMethod     = xQuote.SymbolTradingMethod; 
            if (xQuote.SymbolTradingSantion    == EGlobalConfig.__INIT_NULL_STRING) this.SymbolTradingSantion    = EGlobalConfig.__STRING_BLANK; else this.SymbolTradingSantion    = xQuote.SymbolTradingSantion; 
            if (xQuote.TentativeExecutionPrice == EGlobalConfig.__INIT_NULL_DOUBLE) this.TentativeExecutionPrice = EGlobalConfig.__LONG_0;       else this.TentativeExecutionPrice = xQuote.TentativeExecutionPrice; 
            if (xQuote.ExpectedTradePx         == EGlobalConfig.__INIT_NULL_DOUBLE) this.ExpectedTradePx         = EGlobalConfig.__LONG_0;       else this.ExpectedTradePx         = xQuote.ExpectedTradePx; 
            if (xQuote.ExpectedTradeQty        == EGlobalConfig.__INIT_NULL_DOUBLE) this.ExpectedTradeQty        = EGlobalConfig.__LONG_0;       else this.ExpectedTradeQty        = xQuote.ExpectedTradeQty;
            if (xQuote.BoardEvtID              == EGlobalConfig.__INIT_NULL_STRING) this.BoardEvtID              = EGlobalConfig.__STRING_BLANK; else this.BoardEvtID              = xQuote.BoardEvtID;
            if (xQuote.SessOpenCloseCode       == EGlobalConfig.__INIT_NULL_STRING) this.SessOpenCloseCode       = EGlobalConfig.__STRING_BLANK; else this.SessOpenCloseCode       = xQuote.SessOpenCloseCode;
            if (xQuote.TradingSessionID        == EGlobalConfig.__INIT_NULL_STRING) this.TradingSessionID        = EGlobalConfig.__STRING_BLANK; else this.TradingSessionID        = xQuote.TradingSessionID;
            if (xQuote.TransactTime            == EGlobalConfig.__INIT_NULL_STRING) this.TransactTime            = EGlobalConfig.__STRING_BLANK; else this.TransactTime            = xQuote.TransactTime;
            if (xQuote.TradeDate               == EGlobalConfig.__INIT_NULL_STRING) this.TradeDate               = EGlobalConfig.__STRING_BLANK; else this.TradeDate               = xQuote.TradeDate;            
            return xQuote;
        }
    }

    /// <summary>
    /// tuong tu EXQuote nhung class nay de rieng cho serialize
    /// https://github.com/neuecc/MessagePack-CSharp/issues/859
    /// There is no option to ignore the null value? Like Json.Net?
    /// Correct: MessagePack does not offer that option. => tam thoi bo qua MessagePack  => dung Utf8Json
    /// -------------
    /// using MessagePack;
    /// ---------------
    /// nuget install MessagePack
    /// nuget install MessagePackAnalyzer
    /// </summary>
    
    public class EXQuoteS : EXBase
    {
        [DataMember(Name = "isin")]
        [JsonProperty(PropertyName = "isin", Order = 0)]
        public string? ISIN { get; set; }
        [DataMember(Name = "s")]
        [JsonProperty(PropertyName = "s", Order = 1)]
        public string? Symbol { get; set; }
        [DataMember(Name = "m")]
        [JsonProperty(PropertyName = "m", Order = 2)]
        public string? MarketID { get; set; }
        [DataMember(Name = "b")]
        [JsonProperty(PropertyName = "b", Order = 2)]
        public string? BoardID { get; set; }
        [DataMember(Name = "r")]
        [JsonProperty(PropertyName = "r", Order = 3)]
        public double? ReferencePrice { get; set; }
        [DataMember(Name = "c")]
        [JsonProperty(PropertyName = "c", Order = 4)]
        public double? CeilingPrice { get; set; }
        [DataMember(Name = "f")]
        [JsonProperty(PropertyName = "f", Order = 5)]
        public double? FloorPrice { get; set; }
        [DataMember(Name = "bc")]
        [JsonProperty(PropertyName = "bc", Order = 6)]
        public long? BidCount { get; set; }
        [DataMember(Name = "tbq")]
        [JsonProperty(PropertyName = "tbq", Order = 7)]
        public long? TotalBidQtty { get; set; }
        [DataMember(Name = "bpx")]
        [JsonProperty(PropertyName = "bpx", Order = 8)]
        public double? BuyPriceX { get; set; }
        [DataMember(Name = "bqx")]
        [JsonProperty(PropertyName = "bqx", Order = 9)]
        public long? BuyQuantityX { get; set; }
        [DataMember(Name = "bp9")]
        [JsonProperty(PropertyName = "bp9", Order = 10)]
        public double? BuyPrice9 { get; set; }
        [DataMember(Name = "bq9")]
        [JsonProperty(PropertyName = "bq9", Order = 11)]
        public long? BuyQuantity9 { get; set; }
        [DataMember(Name = "bp8")]
        [JsonProperty(PropertyName = "bp8", Order = 12)]
        public double? BuyPrice8 { get; set; }
        [DataMember(Name = "bq8")]
        [JsonProperty(PropertyName = "bq8", Order = 13)]
        public long? BuyQuantity8 { get; set; }
        [DataMember(Name = "bp7")]
        [JsonProperty(PropertyName = "bp7", Order = 14)]
        public double? BuyPrice7 { get; set; }
        [DataMember(Name = "bq7")]
        [JsonProperty(PropertyName = "bq7", Order = 15)]
        public long? BuyQuantity7 { get; set; }
        [DataMember(Name = "bp6")]
        [JsonProperty(PropertyName = "bp6", Order = 16)]
        public double? BuyPrice6 { get; set; }
        [DataMember(Name = "bq6")]
        [JsonProperty(PropertyName = "bq6", Order = 17)]
        public long? BuyQuantity6 { get; set; }
        [DataMember(Name = "bp5")]
        [JsonProperty(PropertyName = "bp5", Order = 18)]
        public double? BuyPrice5 { get; set; }
        [DataMember(Name = "bq5")]
        [JsonProperty(PropertyName = "bq5", Order = 19)]
        public long? BuyQuantity5 { get; set; }
        [DataMember(Name = "bp4")]
        [JsonProperty(PropertyName = "bp4", Order = 20)]
        public double? BuyPrice4 { get; set; }
        [DataMember(Name = "bq4")]
        [JsonProperty(PropertyName = "bq4", Order = 21)]
        public long? BuyQuantity4 { get; set; }
        [DataMember(Name = "bp3")]
        [JsonProperty(PropertyName = "bp3", Order = 22)]
        public double? BuyPrice3 { get; set; }
        [DataMember(Name = "bq3")]
        [JsonProperty(PropertyName = "bq3", Order = 23)]
        public long? BuyQuantity3 { get; set; }
        [DataMember(Name = "bp2")]
        [JsonProperty(PropertyName = "bp2", Order = 24)]
        public double? BuyPrice2 { get; set; }
        [DataMember(Name = "bq2")]
        [JsonProperty(PropertyName = "bq2", Order = 25)]
        public long? BuyQuantity2 { get; set; }
        [DataMember(Name = "bp1")]
        [JsonProperty(PropertyName = "bp1", Order = 26)]
        public double? BuyPrice1 { get; set; }
        [DataMember(Name = "bq1")]
        [JsonProperty(PropertyName = "bq1", Order = 27)]
        public long? BuyQuantity1 { get; set; }
        [DataMember(Name = "mp")]
        [JsonProperty(PropertyName = "mp", Order = 28)]
        public double? MatchPrice { get; set; }
        [DataMember(Name = "mq")]
        [JsonProperty(PropertyName = "mq", Order = 29)]
        public long? MatchQuantity { get; set; }
        [DataMember(Name = "mc")]
        [JsonProperty(PropertyName = "mc", Order = 30)]
        public double? MatchChange { get; set; }
        [DataMember(Name = "mcp")]
        [JsonProperty(PropertyName = "mcp", Order = 31)]
        public double? MatchChangePercent { get; set; }
        [DataMember(Name = "tnq")]
        [JsonProperty(PropertyName = "tnq", Order = 32)]
        public long? TotalNMQuantity { get; set; }
        [DataMember(Name = "sp1")]
        [JsonProperty(PropertyName = "sp1", Order = 33)]
        public double? SellPrice1 { get; set; }
        [DataMember(Name = "sq1")]
        [JsonProperty(PropertyName = "sq1", Order = 34)]
        public long? SellQuantity1 { get; set; }
        [DataMember(Name = "sp2")]
        [JsonProperty(PropertyName = "sp2", Order = 35)]
        public double? SellPrice2 { get; set; }
        [DataMember(Name = "sq2")]
        [JsonProperty(PropertyName = "sq2", Order = 36)]
        public long? SellQuantity2 { get; set; }
        [DataMember(Name = "sp3")]
        [JsonProperty(PropertyName = "sp3", Order = 37)]
        public double? SellPrice3 { get; set; }
        [DataMember(Name = "sq3")]
        [JsonProperty(PropertyName = "sq3", Order = 38)]
        public long? SellQuantity3 { get; set; }
        [DataMember(Name = "sp4")]
        [JsonProperty(PropertyName = "sp4", Order = 39)]
        public double? SellPrice4 { get; set; }
        [DataMember(Name = "sq4")]
        [JsonProperty(PropertyName = "sq4", Order = 40)]
        public long? SellQuantity4 { get; set; }
        [DataMember(Name = "sp5")]
        [JsonProperty(PropertyName = "sp5", Order = 41)]
        public double? SellPrice5 { get; set; }
        [DataMember(Name = "sq5")]
        [JsonProperty(PropertyName = "sq5", Order = 42)]
        public long? SellQuantity5 { get; set; }
        [DataMember(Name = "sp6")]
        [JsonProperty(PropertyName = "sp6", Order = 43)]
        public double? SellPrice6 { get; set; }
        [DataMember(Name = "sq6")]
        [JsonProperty(PropertyName = "sq6", Order = 44)]
        public long? SellQuantity6 { get; set; }
        [DataMember(Name = "sp7")]
        [JsonProperty(PropertyName = "sp7", Order = 45)]
        public double? SellPrice7 { get; set; }
        [DataMember(Name = "sq7")]
        [JsonProperty(PropertyName = "sq7", Order = 46)]
        public long? SellQuantity7 { get; set; }
        [DataMember(Name = "sp8")]
        [JsonProperty(PropertyName = "sp8", Order = 47)]
        public double? SellPrice8 { get; set; }
        [DataMember(Name = "sq8")]
        [JsonProperty(PropertyName = "sq8", Order = 48)]
        public long? SellQuantity8 { get; set; }
        [DataMember(Name = "sp9")]
        [JsonProperty(PropertyName = "sp9", Order = 49)]
        public double? SellPrice9 { get; set; }
        [DataMember(Name = "sq9")]
        [JsonProperty(PropertyName = "sq9", Order = 50)]
        public long? SellQuantity9 { get; set; }
        [DataMember(Name = "spx")]
        [JsonProperty(PropertyName = "spx", Order = 51)]
        public double? SellPriceX { get; set; }
        [DataMember(Name = "sqx")]
        [JsonProperty(PropertyName = "sqx", Order = 52)]
        public long? SellQuantityX { get; set; }
        [DataMember(Name = "oc")]
        [JsonProperty(PropertyName = "oc", Order = 53)]
        public long? OfferCount { get; set; }
        [DataMember(Name = "toq")]
        [JsonProperty(PropertyName = "toq", Order = 54)]
        public long? TotalOfferQtty { get; set; }
        [DataMember(Name = "op")]
        [JsonProperty(PropertyName = "op", Order = 55)]
        public double? OpenPrice { get; set; }
        [DataMember(Name = "av")]
        [JsonProperty(PropertyName = "av", Order = 56)]
        public double? AveragePrice { get; set; }
        [DataMember(Name = "hi")]
        [JsonProperty(PropertyName = "hi", Order = 57)]
        public double? HighestPrice { get; set; }
        [DataMember(Name = "lo")]
        [JsonProperty(PropertyName = "lo", Order = 58)]
        public double? LowestPrice { get; set; }
        [DataMember(Name = "fbq")]
        [JsonProperty(PropertyName = "fbq", Order = 59)]
        public long? ForeignBuyQuantity { get; set; }
        [DataMember(Name = "fsq")]
        [JsonProperty(PropertyName = "fsq", Order = 60)]
        public long? ForeignSellQuantity { get; set; }
        [DataMember(Name = "frr")]
        [JsonProperty(PropertyName = "frr", Order = 61)]
        public long? ForeignRoomRemain { get; set; }
        [DataMember(Name = "oi")]
        [JsonProperty(PropertyName = "oi", Order = 62)]
        public long? OpenInterest { get; set; }
        [DataMember(Name = "ltd")]
        [JsonProperty(PropertyName = "ltd", Order = 63)]
        public string? LastTradingDate { get; set; }
        [DataMember(Name = "ect")]
        [JsonProperty(PropertyName = "ect", Order = 64)]
        public string? ExClassType { get; set; }
        [DataMember(Name = "stm")]
        [JsonProperty(PropertyName = "stm", Order = 65)]
        public string? SymbolTradingMethod { get; set; }
        [DataMember(Name = "sts")]
        [JsonProperty(PropertyName = "sts", Order = 66)]
        public string? SymbolTradingSantion { get; set; }
        [DataMember(Name = "tep")]
        [JsonProperty(PropertyName = "tep", Order = 67)]
        public double? TentativeExecutionPrice { get; set; }
        [DataMember(Name = "etp")]
        [JsonProperty(PropertyName = "etp", Order = 68)]
        public double? ExpectedTradePx { get; set; }
        [DataMember(Name = "etq")]
        [JsonProperty(PropertyName = "etq", Order = 69)]
        public long? ExpectedTradeQty { get; set; }
        [DataMember(Name = "be")]
        [JsonProperty(PropertyName = "be", Order = 70)]
        public string? BoardEvtID { get; set; }
        [DataMember(Name = "sc")]
        [JsonProperty(PropertyName = "sc", Order = 71)]
        public string? SessOpenCloseCode { get; set; }
        [DataMember(Name = "ts")]
        [JsonProperty(PropertyName = "ts", Order = 72)]
        public string? TradingSessionID { get; set; }
        [DataMember(Name = "tt")]
        [JsonProperty(PropertyName = "tt", Order = 73)]
        public string? TransactTime { get; set; }
        [DataMember(Name = "td")]
        [JsonProperty(PropertyName = "td", Order = 74)]
        public string? TradeDate { get; set; }
#nullable disable

        /// <summary>
        /// cac truong hop data raw voi field ko co data thi khi serialize se ko output ra field do
        /// chu y truong hop data tai cot top 10 buy sell, gia tri null khac gia tri 0
        /// + gia tri 0    la xoa data tai do do
        /// + gia tri null la bo qua data tai o do ( co the co dang co hoac ko co data )
        /// </summary>
        /// <param name="xQuote"></param>
        public EXQuoteS(EXQuote xQuote)
        {
            if (xQuote.ISIN                    == EGlobalConfig.__INIT_NULL_STRING) this.ISIN                    = null; else this.ISIN                    = xQuote.ISIN; 
            if (xQuote.Symbol                  == EGlobalConfig.__INIT_NULL_STRING) this.Symbol                  = null; else this.Symbol                  = xQuote.Symbol; 
            if (xQuote.MarketID                == EGlobalConfig.__INIT_NULL_STRING) this.MarketID                = null; else this.MarketID                = xQuote.MarketID; 
            if (xQuote.BoardID                 == EGlobalConfig.__INIT_NULL_STRING) this.BoardID                 = null; else this.BoardID                 = xQuote.BoardID; 
            if (xQuote.ReferencePrice          == EGlobalConfig.__INIT_NULL_DOUBLE) this.ReferencePrice          = null; else this.ReferencePrice          = xQuote.ReferencePrice; 
            if (xQuote.CeilingPrice            == EGlobalConfig.__INIT_NULL_DOUBLE) this.CeilingPrice            = null; else this.CeilingPrice            = xQuote.CeilingPrice; 
            if (xQuote.FloorPrice              == EGlobalConfig.__INIT_NULL_DOUBLE) this.FloorPrice              = null; else this.FloorPrice              = xQuote.FloorPrice; 
            if (xQuote.BidCount                == EGlobalConfig.__INIT_NULL_LONG)   this.BidCount                = null; else this.BidCount                = xQuote.BidCount; 
            if (xQuote.TotalBidQtty            == EGlobalConfig.__INIT_NULL_LONG)   this.TotalBidQtty            = null; else this.TotalBidQtty            = xQuote.TotalBidQtty; 
            if (xQuote.BuyPriceX               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPriceX               = null; else this.BuyPriceX               = xQuote.BuyPriceX; 
            if (xQuote.BuyQuantityX            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantityX            = null; else this.BuyQuantityX            = xQuote.BuyQuantityX; 
            if (xQuote.BuyPrice9               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice9               = null; else this.BuyPrice9               = xQuote.BuyPrice9; 
            if (xQuote.BuyQuantity9            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity9            = null; else this.BuyQuantity9            = xQuote.BuyQuantity9; 
            if (xQuote.BuyPrice8               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice8               = null; else this.BuyPrice8               = xQuote.BuyPrice8; 
            if (xQuote.BuyQuantity8            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity8            = null; else this.BuyQuantity8            = xQuote.BuyQuantity8; 
            if (xQuote.BuyPrice7               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice7               = null; else this.BuyPrice7               = xQuote.BuyPrice7; 
            if (xQuote.BuyQuantity7            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity7            = null; else this.BuyQuantity7            = xQuote.BuyQuantity7; 
            if (xQuote.BuyPrice6               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice6               = null; else this.BuyPrice6               = xQuote.BuyPrice6; 
            if (xQuote.BuyQuantity6            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity6            = null; else this.BuyQuantity6            = xQuote.BuyQuantity6; 
            if (xQuote.BuyPrice5               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice5               = null; else this.BuyPrice5               = xQuote.BuyPrice5; 
            if (xQuote.BuyQuantity5            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity5            = null; else this.BuyQuantity5            = xQuote.BuyQuantity5; 
            if (xQuote.BuyPrice4               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice4               = null; else this.BuyPrice4               = xQuote.BuyPrice4; 
            if (xQuote.BuyQuantity4            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity4            = null; else this.BuyQuantity4            = xQuote.BuyQuantity4; 
            if (xQuote.BuyPrice3               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice3               = null; else this.BuyPrice3               = xQuote.BuyPrice3; 
            if (xQuote.BuyQuantity3            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity3            = null; else this.BuyQuantity3            = xQuote.BuyQuantity3; 
            if (xQuote.BuyPrice2               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice2               = null; else this.BuyPrice2               = xQuote.BuyPrice2; 
            if (xQuote.BuyQuantity2            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity2            = null; else this.BuyQuantity2            = xQuote.BuyQuantity2; 
            if (xQuote.BuyPrice1               == EGlobalConfig.__INIT_NULL_DOUBLE) this.BuyPrice1               = null; else this.BuyPrice1               = xQuote.BuyPrice1; 
            if (xQuote.BuyQuantity1            == EGlobalConfig.__INIT_NULL_LONG)   this.BuyQuantity1            = null; else this.BuyQuantity1            = xQuote.BuyQuantity1; 
            if (xQuote.MatchPrice              == EGlobalConfig.__INIT_NULL_DOUBLE) this.MatchPrice              = null; else this.MatchPrice              = xQuote.MatchPrice; 
            if (xQuote.MatchQuantity           == EGlobalConfig.__INIT_NULL_LONG)   this.MatchQuantity           = null; else this.MatchQuantity           = xQuote.MatchQuantity; 
            if (xQuote.MatchChange             == EGlobalConfig.__INIT_NULL_DOUBLE) this.MatchChange             = null; else this.MatchChange             = xQuote.MatchChange; 
            if (xQuote.MatchChangePercent      == EGlobalConfig.__INIT_NULL_DOUBLE) this.MatchChangePercent      = null; else this.MatchChangePercent      = xQuote.MatchChangePercent; 
            if (xQuote.TotalNMQuantity         == EGlobalConfig.__INIT_NULL_LONG)   this.TotalNMQuantity         = null; else this.TotalNMQuantity         = xQuote.TotalNMQuantity; 
            if (xQuote.SellPrice1              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice1              = null; else this.SellPrice1              = xQuote.SellPrice1; 
            if (xQuote.SellQuantity1           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity1           = null; else this.SellQuantity1           = xQuote.SellQuantity1; 
            if (xQuote.SellPrice2              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice2              = null; else this.SellPrice2              = xQuote.SellPrice2; 
            if (xQuote.SellQuantity2           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity2           = null; else this.SellQuantity2           = xQuote.SellQuantity2; 
            if (xQuote.SellPrice3              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice3              = null; else this.SellPrice3              = xQuote.SellPrice3; 
            if (xQuote.SellQuantity3           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity3           = null; else this.SellQuantity3           = xQuote.SellQuantity3; 
            if (xQuote.SellPrice4              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice4              = null; else this.SellPrice4              = xQuote.SellPrice4; 
            if (xQuote.SellQuantity4           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity4           = null; else this.SellQuantity4           = xQuote.SellQuantity4; 
            if (xQuote.SellPrice5              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice5              = null; else this.SellPrice5              = xQuote.SellPrice5; 
            if (xQuote.SellQuantity5           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity5           = null; else this.SellQuantity5           = xQuote.SellQuantity5; 
            if (xQuote.SellPrice6              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice6              = null; else this.SellPrice6              = xQuote.SellPrice6; 
            if (xQuote.SellQuantity6           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity6           = null; else this.SellQuantity6           = xQuote.SellQuantity6; 
            if (xQuote.SellPrice7              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice7              = null; else this.SellPrice7              = xQuote.SellPrice7; 
            if (xQuote.SellQuantity7           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity7           = null; else this.SellQuantity7           = xQuote.SellQuantity7; 
            if (xQuote.SellPrice8              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice8              = null; else this.SellPrice8              = xQuote.SellPrice8; 
            if (xQuote.SellQuantity8           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity8           = null; else this.SellQuantity8           = xQuote.SellQuantity8; 
            if (xQuote.SellPrice9              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPrice9              = null; else this.SellPrice9              = xQuote.SellPrice9; 
            if (xQuote.SellQuantity9           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantity9           = null; else this.SellQuantity9           = xQuote.SellQuantity9; 
            if (xQuote.SellPriceX              == EGlobalConfig.__INIT_NULL_DOUBLE) this.SellPriceX              = null; else this.SellPriceX              = xQuote.SellPriceX; 
            if (xQuote.SellQuantityX           == EGlobalConfig.__INIT_NULL_LONG)   this.SellQuantityX           = null; else this.SellQuantityX           = xQuote.SellQuantityX; 
            if (xQuote.OfferCount              == EGlobalConfig.__INIT_NULL_LONG)   this.OfferCount              = null; else this.OfferCount              = xQuote.OfferCount; 
            if (xQuote.TotalOfferQtty          == EGlobalConfig.__INIT_NULL_LONG)   this.TotalOfferQtty          = null; else this.TotalOfferQtty          = xQuote.TotalOfferQtty; 
            if (xQuote.OpenPrice               == EGlobalConfig.__INIT_NULL_DOUBLE) this.OpenPrice               = null; else this.OpenPrice               = xQuote.OpenPrice; 
            if (xQuote.AveragePrice            == EGlobalConfig.__INIT_NULL_DOUBLE) this.AveragePrice            = null; else this.AveragePrice            = xQuote.AveragePrice; 
            if (xQuote.HighestPrice            == EGlobalConfig.__INIT_NULL_DOUBLE) this.HighestPrice            = null; else this.HighestPrice            = xQuote.HighestPrice; 
            if (xQuote.LowestPrice             == EGlobalConfig.__INIT_NULL_DOUBLE) this.LowestPrice             = null; else this.LowestPrice             = xQuote.LowestPrice; 
            if (xQuote.ForeignBuyQuantity      == EGlobalConfig.__INIT_NULL_LONG)   this.ForeignBuyQuantity      = null; else this.ForeignBuyQuantity      = xQuote.ForeignBuyQuantity; 
            if (xQuote.ForeignSellQuantity     == EGlobalConfig.__INIT_NULL_LONG)   this.ForeignSellQuantity     = null; else this.ForeignSellQuantity     = xQuote.ForeignSellQuantity; 
            if (xQuote.ForeignRoomRemain       == EGlobalConfig.__INIT_NULL_DOUBLE) this.ForeignRoomRemain       = null; else this.ForeignRoomRemain       = xQuote.ForeignRoomRemain; 
            if (xQuote.OpenInterest            == EGlobalConfig.__INIT_NULL_DOUBLE) this.OpenInterest            = null; else this.OpenInterest            = xQuote.OpenInterest; 
            if (xQuote.LastTradingDate         == EGlobalConfig.__INIT_NULL_STRING) this.LastTradingDate         = null; else this.LastTradingDate         = xQuote.LastTradingDate; 
            if (xQuote.ExClassType             == EGlobalConfig.__INIT_NULL_STRING) this.ExClassType             = null; else this.ExClassType             = xQuote.ExClassType; 
            if (xQuote.SymbolTradingMethod     == EGlobalConfig.__INIT_NULL_STRING) this.SymbolTradingMethod     = null; else this.SymbolTradingMethod     = xQuote.SymbolTradingMethod; 
            if (xQuote.SymbolTradingSantion    == EGlobalConfig.__INIT_NULL_STRING) this.SymbolTradingSantion    = null; else this.SymbolTradingSantion    = xQuote.SymbolTradingSantion; 
            if (xQuote.TentativeExecutionPrice == EGlobalConfig.__INIT_NULL_DOUBLE) this.TentativeExecutionPrice = null; else this.TentativeExecutionPrice = xQuote.TentativeExecutionPrice; 
            if (xQuote.ExpectedTradePx         == EGlobalConfig.__INIT_NULL_DOUBLE) this.ExpectedTradePx         = null; else this.ExpectedTradePx         = xQuote.ExpectedTradePx; 
            if (xQuote.ExpectedTradeQty        == EGlobalConfig.__INIT_NULL_DOUBLE) this.ExpectedTradeQty        = null; else this.ExpectedTradeQty        = xQuote.ExpectedTradeQty;
            if (xQuote.BoardEvtID              == EGlobalConfig.__INIT_NULL_STRING) this.BoardEvtID              = null; else this.BoardEvtID              = xQuote.BoardEvtID;
            if (xQuote.SessOpenCloseCode       == EGlobalConfig.__INIT_NULL_STRING) this.SessOpenCloseCode       = null; else this.SessOpenCloseCode       = xQuote.SessOpenCloseCode;
            if (xQuote.TradingSessionID        == EGlobalConfig.__INIT_NULL_STRING) this.TradingSessionID        = null; else this.TradingSessionID        = xQuote.TradingSessionID;
            if (xQuote.TransactTime            == EGlobalConfig.__INIT_NULL_STRING) this.TransactTime            = null; else this.TransactTime            = xQuote.TransactTime;
            if (xQuote.TradeDate               == EGlobalConfig.__INIT_NULL_STRING) this.TradeDate               = null; else this.TradeDate               = xQuote.TradeDate;                       


            // co gia tham chieu va gia khop thi tinh ra gia thay doi (MC) va thay doi phan tram (MCP)
            if (xQuote.ReferencePrice != EGlobalConfig.__INIT_NULL_DOUBLE && xQuote.MatchPrice != EGlobalConfig.__INIT_NULL_DOUBLE && xQuote.ReferencePrice != 0)
            {
                this.MatchChange        = xQuote.MatchPrice - xQuote.ReferencePrice;
                this.MatchChange        = System.Math.Round(Convert.ToDouble(this.MatchChange), 2); // -6.76
                this.MatchChangePercent = (this.MatchChange / xQuote.ReferencePrice) * 100; // -6.756756756756757
                this.MatchChangePercent = System.Math.Round(Convert.ToDouble( this.MatchChangePercent) , 2); // -6.76
            }
        }
        
    }
}
