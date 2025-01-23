namespace MDDSCore.Boards
{
    /// <summary>
    /// fastest serialize / deserialize
    /// https://github.com/neuecc/Utf8Json
    /// </summary>
	public abstract class BBase  
	{
        // ==========================================================================================================
        // ===================================== SHORT NAME==========================================================
        // ==========================================================================================================

        // market status
        public const string __SHORT_MARKETID         = "m";
        public const string __SHORT_BOARDID          = "b";
        public const string __SHORT_BOARDEVTID       = "be";
        public const string __SHORT_TRADINGSESSIONID = "ts";

        // basic
        public const string __SHORT_SYMBOL         = "s";
        public const string __SHORT_REFERENCEPRICE = "r";
        public const string __SHORT_CEILINGPRICE   = "c";
        public const string __SHORT_FLOORPRICE     = "f";

        // basket
        public const string __SHORT_NAME = "n";
        public const string __SHORT_LIST = "l";
        
        // index
        public const string __SHORT_TRANSACTTIME     = "tt";
        public const string __SHORT_INDEX            = "i";
        public const string __SHORT_VALUE            = "v";
        public const string __SHORT_CHANGE           = "c";
        public const string __SHORT_CHANGEPERCENT    = "cp";
        public const string __SHORT_TOTALQUANTITY    = "tq";
        public const string __SHORT_TOTALVALUE       = "tv";
        public const string __SHORT_NMTOTALQUANTITY  = "nmtq";
        public const string __SHORT_NMTOTALVALUE     = "nmtv";
        public const string __SHORT_PTTOTALQUANTITY  = "pttq";
        public const string __SHORT_PTTOTALVALUE     = "pttv";
        public const string __SHORT_CEILINGCOUNT     = "cc";
        public const string __SHORT_UPCOUNT          = "uc";
        public const string __SHORT_NOCHANGECOUNT    = "nc";
        public const string __SHORT_DOWNCOUNT        = "dc";
        public const string __SHORT_FLOORCOUNT       = "fc";

        // quote
        public const string __SHORT_REFERENCE               = "r";
        public const string __SHORT_CEILING                 = "c";
        public const string __SHORT_FLOOR                   = "f";
        public const string __SHORT_BIDCOUNT                = "bc";
        public const string __SHORT_TOTALBIDQTTY            = "tbq";
        public const string __SHORT_BUYPRICEX               = "bpx";
        public const string __SHORT_BUYQUANTITYX            = "bqx";
        public const string __SHORT_BUYPRICE9               = "bp9";
        public const string __SHORT_BUYQUANTITY9            = "bq9";
        public const string __SHORT_BUYPRICE8               = "bp8";
        public const string __SHORT_BUYQUANTITY8            = "bq8";
        public const string __SHORT_BUYPRICE7               = "bp7";
        public const string __SHORT_BUYQUANTITY7            = "bq7";
        public const string __SHORT_BUYPRICE6               = "bp6";
        public const string __SHORT_BUYQUANTITY6            = "bq6";
        public const string __SHORT_BUYPRICE5               = "bp5";
        public const string __SHORT_BUYQUANTITY5            = "bq5";
        public const string __SHORT_BUYPRICE4               = "bp4";
        public const string __SHORT_BUYQUANTITY4            = "bq4";
        public const string __SHORT_BUYPRICE3               = "bp3";
        public const string __SHORT_BUYQUANTITY3            = "bq3";
        public const string __SHORT_BUYPRICE2               = "bp2";
        public const string __SHORT_BUYQUANTITY2            = "bq2";
        public const string __SHORT_BUYPRICE1               = "bp1";
        public const string __SHORT_BUYQUANTITY1            = "bq1";
        public const string __SHORT_MATCHPRICE              = "mp";
        public const string __SHORT_MATCHQUANTITY           = "mq";
        public const string __SHORT_MATCHCHANGE             = "mc";
        public const string __SHORT_MATCHCHANGEPERCENT      = "mcp";
        public const string __SHORT_TOTALNMQUANTITY         = "tnq";
        public const string __SHORT_SELLPRICE1              = "sp1";
        public const string __SHORT_SELLQUANTITY1           = "sq1";
        public const string __SHORT_SELLPRICE2              = "sp2";
        public const string __SHORT_SELLQUANTITY2           = "sq2";
        public const string __SHORT_SELLPRICE3              = "sp3";
        public const string __SHORT_SELLQUANTITY3           = "sq3";
        public const string __SHORT_SELLPRICE4              = "sp4";
        public const string __SHORT_SELLQUANTITY4           = "sq4";
        public const string __SHORT_SELLPRICE5              = "sp5";
        public const string __SHORT_SELLQUANTITY5           = "sq5";
        public const string __SHORT_SELLPRICE6              = "sp6";
        public const string __SHORT_SELLQUANTITY6           = "sq6";
        public const string __SHORT_SELLPRICE7              = "sp7";
        public const string __SHORT_SELLQUANTITY7           = "sq7";
        public const string __SHORT_SELLPRICE8              = "sp8";
        public const string __SHORT_SELLQUANTITY8           = "sq8";
        public const string __SHORT_SELLPRICE9              = "sp9";
        public const string __SHORT_SELLQUANTITY9           = "sq9";
        public const string __SHORT_SELLPRICEX              = "spx";
        public const string __SHORT_SELLQUANTITYX           = "sqx";
        public const string __SHORT_OFFERCOUNT              = "oc";
        public const string __SHORT_TOTALOFFERQTTY          = "toq";
        public const string __SHORT_OPENPRICE               = "op";
        public const string __SHORT_AVERAGEPRICE            = "av";
        public const string __SHORT_HIGHESTPRICE            = "hi";
        public const string __SHORT_LOWESTPRICE             = "lo";
        public const string __SHORT_FOREIGNBUYQUANTITY      = "fbq";
        public const string __SHORT_FOREIGNSELLQUANTITY     = "fsq";
        public const string __SHORT_FOREIGNROOMREMAIN       = "frr";
        public const string __SHORT_OPENINTEREST            = "oi";
        public const string __SHORT_LASTTRADINGDATE         = "ltd";
        public const string __SHORT_EXCLASSTYPE             = "ect";
        public const string __SHORT_SYMBOLTRADINGMETHOD     = "stm";
        public const string __SHORT_SYMBOLTRADINGSANCTION   = "sts";
        public const string __SHORT_TENTATIVEEXECUTIONPRICE = "tep";
        public const string __SHORT_EXPECTEDTRADEPX         = "etp";
        public const string __SHORT_EXPECTEDTRADEQTY        = "etq";
    }
}
