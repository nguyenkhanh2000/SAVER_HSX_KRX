using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.Stock6G.Entities
{
    /// <summary>
    /// luu thong tin index hom truoc
    /// {"Time":"2021-04-23 08:26:52.454","Data":[{"IndexNane":"VN30","TradingDate":"22-04-2021","LastIndex":1271.53}]}
    /// </summary>
    public class ELastIndex
    {
        /// <summary>
        //30001=STO---30167=001---30632=VNINDEX
        //30001=STO---30167=101---30632=VN30
        //30001=STO---30167=102---30632=VNMIDCAP
        //30001=STO---30167=103---30632=VNSMALLCAP
        //30001=STO---30167=104---30632=VN100
        //30001=STO---30167=105---30632=VNALLSHARE
        //30001=STO---30167=151---30632=VNXALLSHARE
        //30001=STO---30167=152---30632=VNX50
        //30001=STO---30167=301---30632=VNAllShare Consumer Discretionary
        //30001=STO---30167=302---30632=VNAllShare Consumer Staples
        //30001=STO---30167=303---30632=VNAllShare Energy
        //30001=STO---30167=304---30632=VNAllShare Financials
        //30001=STO---30167=305---30632=VNAllShare Health Care
        //30001=STO---30167=306---30632=VNAllShare Industrials
        //30001=STO---30167=307---30632=VNAllShare Information Technology
        //30001=STO---30167=308---30632=VNAllShare Materials
        //30001=STO---30167=309---30632=VNAllShare Real Estate
        //30001=STO---30167=310---30632=VNAllShare Utilities
        //30001=STO---30167=401---30632=VN Sustainability Index
        //30001=STO---30167=501---30632=VN Diamond Index
        //30001=STO---30167=502---30632=VN Financial Select Sector Index
        //30001=STO---30167=503---30632=VN Leading Financial Index
        //30001=STO---30167=990---30632=PD TEST01
        //--------------------------------------------------------------------
        //30001=STX---30167=001---30632=HNX Composite Index
        //30001=STX---30167=002---30632=HNXIndex
        //30001=STX---30167=003---30632=HNX Large Cap Index
        //30001=STX---30167=004---30632=HNX Mid/Small Cap Index
        //30001=STX---30167=100---30632=HNX30
        //30001=STX---30167=200---30632=HNX Manufacturing Index
        //30001=STX---30167=201---30632=HNX Construction Index
        //30001=STX---30167=202---30632=HNX Financials Index
        //30001=UPX---30167=001---30632=UPCOM Composite
        //30001=UPX---30167=301---30632=HNXUpcomIndex
        //30001=UPX---30167=310---30632=UPCOM Large Index
        //30001=UPX---30167=320---30632=UPCOM Small Index
        //30001=UPX---30167=330---30632=UPCOM Medium Index

        /// </summary>
        public string IndexCode { get; set; }
        /// <summary>
        /// VN30
        /// </summary>
        public string IndexNane { get; set; }
        /// <summary>
        /// 22-04-2021
        /// </summary>
        public string TradingDate { get; set; }
        /// <summary>
        /// 1271.53
        /// </summary>
        public double LastIndex { get; set; }        
    }
}
