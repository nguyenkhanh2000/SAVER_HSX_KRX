using CommonLib.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace PriceLib.Implementations
{
	/// <summary>
	/// 2020-07-24 10:53:32 ngocta2
	/// base class cho cac class oracle, mssql thua ke de insert vao db oracle va mssql
	/// </summary>
	public abstract class CBasePrice
	{
		// const cho cac function DAL (exec sp)

		protected const string __ABEGINSTRING                                 = "aBeginString";
		protected const string __ASYMBOL                                      = "aSymbol";
		protected const string __ATICKERCODE                                  = "aTickerCode";
		protected const string __ABODYLENGTH                                  = "aBodyLength";
		protected const string __AMSGTYPE                                     = "aMsgType";
		protected const string __ASENDERCOMPID                                = "aSenderCompID";
		protected const string __ATARGETCOMPID                                = "aTargetCompID";
		protected const string __AMSGSEQNUM                                   = "aMsgSeqNum";
		protected const string __ASENDINGTIME                                 = "aSendingTime";
		protected const string __AMARKETID                                    = "aMarketID";
        protected const string __ACREATEDATE                                  = "aCreateTime";
        protected const string __ABOARDID                                     = "aBoardID";
		protected const string __ATOTNUMREPORTS                               = "aTotNumReports";
		protected const string __ASECURITYEXCHANGE                            = "aSecurityExchange";
		protected const string __ASYMBOLSHORTCODE                             = "aSymbolShortCode";
		protected const string __ASYMBOLNAME                                  = "aSymbolName";
		protected const string __ASYMBOLENGLISHNAME                           = "aSymbolEnglishName";
		protected const string __APRODUCTID                                   = "aProductID";
		protected const string __APRODUCTGRPID                                = "aProductGrpID";
		protected const string __ASECURITYGROUPID                             = "aSecurityGroupID";
		protected const string __APUTORCALL                                   = "aPutOrCall";
		protected const string __AEXERCISESTYLE                               = "aExerciseStyle";
		protected const string __AMATURITYMONTHYEAR                           = "aMaturityMonthYear";
		protected const string __AMATURITYDATE                                = "aMaturityDate";
		protected const string __AISSUER                                      = "aIssuer";
		protected const string __AISSUEDATE                                   = "aIssueDate";
		protected const string __ACONTRACTMULTIPLIER                          = "aContractMultiplier";
		protected const string __ACOUPONRATE                                  = "aCouponRate";
		protected const string __ACURRENCY                                    = "aCurrency";
		protected const string __ALISTEDSHARES                                = "aListedShares";
		protected const string __AHIGHLIMITPRICE                              = "aHighLimitPrice";
		protected const string __ALOWLIMITPRICE                               = "aLowLimitPrice";
		protected const string __ASTRIKEPRICE                                 = "aStrikePrice";
		protected const string __ASECURITYSTATUS                              = "aSecurityStatus";
		protected const string __ACONTRACTSIZE                                = "aContractSize";
		protected const string __ASETTLMETHOD                                 = "aSettlMethod";
		protected const string __AYIELD                                       = "aYield";
		protected const string __AREFERENCEPRICE                              = "aReferencePrice";
		protected const string __AEVALUATIONPRICE                             = "aEvaluationPrice";
		protected const string __AHGSTORDERPRICE                              = "aHgstOrderPrice";
		protected const string __ALWSTORDERPRICE                              = "aLwstOrderPrice";
		protected const string __APREVCLOSEPX                                 = "aPrevClosePx";
		protected const string __ASYMBOLCLOSEINFOPXTYPE                       = "aSymbolCloseInfoPxType";
		protected const string __AFIRSTTRADINGDATE                            = "aFirstTradingDate";
		protected const string __AFINALTRADEDATE                              = "aFinalTradeDate";
		protected const string __AFINALSETTLEDATE                             = "aFinalSettleDate";
		protected const string __ALISTINGDATE                                 = "aListingDate";
		protected const string __AOPENINTERESTQTY                             = "aOpenInterestQty";
		protected const string __ASETTLEMENTPRICE                             = "aSettlementPrice";
		protected const string __ARETRIGGERINGCONDITIONCODE                   = "aREtriggeringconditioncode";
		protected const string __AEXCLASSTYPE                                 = "aExClassType";
		protected const string __AVWAP                                        = "aVWAP";
		protected const string __ASYMBOLADMINSTATUSCODE                       = "aSymbolAdminStatusCode";
		protected const string __ASYMBOLTRADINGMETHODSC                       = "aSymbolTradingMethodSC";
		protected const string __ASYMBOLTRADINGSANTIONSC                      = "aSymbolTradingSantionSC";
		protected const string __ACHECKSUM                                    = "aCheckSum";
		protected const string __ABOARDEVTID                                  = "aBoardEvtID";
		protected const string __ASESSOPENCLOSECODE                           = "aSessOpenCloseCode";
		protected const string __ATRADINGSESSIONID                            = "aTradingSessionID";
		protected const string __ASYMBOLCLOSEINFOPX                           = "aSymbolCloseInfoPx";
		protected const string __ASYMBOLCLOSEINFOYIELD                        = "aSymbolCloseInfoYield";
		protected const string __AVITYPECODE                                  = "aVITypeCode";
		protected const string __AVIKINDCODE                                  = "aVIKindCode";
		protected const string __ASTATICVIBASEPRICE                           = "aStaticVIBasePrice";
		protected const string __ADYNAMICVIBASEPRICE                          = "aDynamicVIBasePrice";
		protected const string __AVIPRICE                                     = "aVIPrice";
		protected const string __ASTATICVIDISPARTIYRATIO                      = "aStaticVIDispartiyRatio";
		protected const string __ADYNAMICVIDISPARTIYRATIO                     = "aDynamicVIDispartiyRatio";
		protected const string __AMARKETMAKERCONTRACTCODE                     = "aMarketMakerContractCode";
		protected const string __AMEMBERNO                                    = "aMemberNo";
		protected const string __AEVENTKINDCODE                               = "aEventKindCode";
		protected const string __AEVENTOCCURRENCEREASONCODE                   = "aEventOccurrenceReasonCode";
		protected const string __AEVENTSTARTDATE                              = "aEventStartDate";
		protected const string __AEVENTENDDATE                                = "aEventEndDate";
		protected const string __AINDEXSTYPECODE                              = "aIndexsTypeCode";
		protected const string __AIDXNAME                                     = "aIdxName";
		protected const string __AIDXENGLISHNAME                              = "aIdxEnglishName";
		protected const string __ATOTALMSGNO                                  = "aTotalMsgNo";
		protected const string __ACURRENTMSGNO                                = "aCurrentMsgNo";
		protected const string __AMARKETINDEXCLASS                            = "aMarketIndexClass";
		protected const string __ATRANSACTTIME                                = "aTransactTime";
		protected const string __RANDOMENDAPPLYCLASSIFICATION                 = "aREApplyClassification";
		protected const string __RANDOMENDTENTATIVEEXECUTIONPRICE             = "aRETentativeexecutionprice";
		protected const string __RANDOMENDESTIMATEDHIGHESTPRICE               = "aREEstimatedHighestprice";
		protected const string __RANDOMENDESTIMATEDHIGHESTPRICEDISPARATERATIO = "aREEHighestPriceDisparateR";
		protected const string __RANDOMENDESTIMATEDLOWESTPRICE                = "aREEstimatedLowestPrice";
		protected const string __RANDOMENDESTIMATEDLOWESTPRICEDISPARATERATIO  = "aREELowestPriceDisparateR";
		protected const string __ALATESTPRICE                                 = "aLatestPrice";
		protected const string __ALATESTPRICEDISPARATERATIO                   = "aLatestPriceDisparateRatio";
		protected const string __ARANDOMENDRELEASETIME                        = "aRandomEndReleaseTime";
		protected const string __ATOTALVOLUMETRADED                           = "aTotalVolumeTraded";
		protected const string __AGROSSTRADEAMT                               = "aGrossTradeAmt";
		protected const string __ASELLTOTORDERQTY                             = "aSellTotOrderQty ";
		protected const string __ABUYTOTORDERQTY                              = "aBuyTotOrderQty ";
		protected const string __ASELLVALIDORDERCNT                           = "aSellValidOrderCnt";
		protected const string __ABUYVALIDORDERCNT                            = "aBuyValidOrderCnt";
		protected const string __ATRADEDATE                                   = "aTradeDate";
		protected const string __ANOMDENTRIES                                 = "aNoMDEntries";
		protected const string __ARPTSEQ                                      = "aRptSeq";
		protected const string __AMDUPDATEACTION                              = "aMDUpdateAction";
		protected const string __AMDENTRYTYPE                                 = "aMDEntryType";
		protected const string __AMDENTRYPOSITIONNO                           = "aMDEntryPositionNo";
		protected const string __AMDENTRYPX                                   = "aMDEntryPx";
		protected const string __AMDENTRYSIZE                                 = "aMDEntrySize";
		protected const string __ANUMBEROFORDERS                              = "aNumberOfOrders";
		protected const string __AMDENTRYYIELD                                = "aMDEntryYield";
		protected const string __AMDENTRYMMSIZE                               = "aMDEntryMMSize";
		protected const string __AINVESTCODE                                  = "aInvestCode";
		protected const string __ASELLVOLUME                                  = "aSellVolume";
		protected const string __ASELLTRADEAMOUNT                             = "aSellTradeAmount";
		protected const string __ABUYVOLUME                                   = "aBuyVolume";
		protected const string __ABUYTRADEDAMOUNT                             = "aBuyTradedAmount";
		protected const string __ABONDCLASSIFICATIONCODE                      = "aBondClassificationCode";
		protected const string __AVALUEINDEXES                                = "aValueIndexes";
		protected const string __ACONTAUCTACCTRDVOL                           = "aContauctAccTrdvol";
		protected const string __ACONTAUCTACCTRDVAL                           = "aContauctAccTrdval";
		protected const string __ABLKTRDACCTRDVOL                             = "aBlktrdAccTrdvol";
		protected const string __ABLKTRDACCTRDVAL                             = "aBlktrdAccTrdval";
		protected const string __AFLUCTUATIONUPPERLIMITISSUECOUNT             = "aFluctuationUpperLimitIC";
		protected const string __AFLUCTUATIONUPISSUECOUNT                     = "aFluctuationUpIC";
		protected const string __AFLUCTUATIONSTEADINESSISSUECOUNT             = "aFluctuationSteadinessIC";
		protected const string __AFLUCTUATIONDOWNISSUECOUNT                   = "aFluctuationDownIC";
		protected const string __AFLUCTUATIONLOWERLIMITISSUECOUNT             = "aFluctuationLowerLimitIC";
		protected const string __AFLUCTUATIONUPISSUEVOLUME                    = "aFluctuationUpIV";
		protected const string __AFLUCTUATIONDOWNISSUEVOLUME                  = "aFluctuationDownIV";
		protected const string __AFLUCTUATIONSTEADINESSISSUEVOLUME            = "aFluctuationSteadinessIV";
		protected const string __ASELLRANKSEQ                                 = "aSellRankSeq";
		protected const string __ASELLMEMBERNO                                = "aSellMemberNo";
		protected const string __ABUYRANKSEQ                                  = "aBuyRankSeq";
		protected const string __ABUYMEMBERNO                                 = "aBuyMemberNo";
		protected const string __AEXPECTEDTRADEPX                             = "aExpectedTradePx";
		protected const string __AEXPECTEDTRADEQTY                            = "aExpectedTradeQty";
		protected const string __AEXPECTEDTRADEYIELD                          = "aExpectedTradeYield";
		protected const string __AFOREIGNERBUYPOSBLQTY                        = "aForeignerBuyPosblQty";
		protected const string __AFOREIGNERORDERLIMITQTY                      = "aForeignerOrderLimitQty";
		protected const string __AINAVVALUE                                   = "aiNAVvalue";
		protected const string __ATRACKINGERROR                               = "aTrackingError";
		protected const string __ADISPARATERATIO                              = "aDisparateRatio";
		protected const string __ARANK                                        = "aRank";
		protected const string __APRICEFLUCTUATIONRATIO                       = "aPriceFluctuationRatio";
		protected const string __AFORNINVESTTYPECODE                          = "aFornInvestTypeCode";
		protected const string __ASELLVOLUMETOTAL                             = "aSellVolumeTotal";
		protected const string __ASELLTRADEAMOUNTTOTAL                        = "aSellTradeAmountTotal";
		protected const string __ABUYVOLUMETOTAL                              = "aBuyVolumeTotal";
		protected const string __ABUYTRADEDAMOUNTTOTAL                        = "aBuyTradedAmountTotal";
		protected const string __ADISCLOSUREID                                = "aDisclosureID";
		protected const string __ALANQUAGECATEGORY                            = "aLanquageCategory";
		protected const string __ADATACATEGORY                                = "aDataCategory";
		protected const string __APUBLICINFORMATIONDATE                       = "aPublicInformationDate";
		protected const string __ATRANSMISSIONDATE                            = "aTransmissionDate";
		protected const string __APROCESSTYPE                                 = "aProcessType";
		protected const string __AHEADLINE                                    = "aHeadline";
		protected const string __ABODY                                        = "aBody";
		protected const string __ABUYPRICE1                                   = "aBuyPrice1";
		protected const string __ABUYQUANTITY1                                = "aBuyQuantity1";
		protected const string __ABUYPRICE1_NOO                               = "aBuyPrice1_NOO";
		protected const string __ABUYPRICE1_MDEY                              = "aBuyPrice1_MDEY";
		protected const string __ABUYPRICE1_MDEMMS                            = "aBuyPrice1_MDEMMS";
		protected const string __ASELLPRICE1                                  = "aSellPrice1";
		protected const string __ASELLQUANTITY1                               = "aSellQuantity1";
		protected const string __ASELLPRICE1_NOO                              = "aSellPrice1_NOO";
		protected const string __ASELLPRICE1_MDEY                             = "aSellPrice1_MDEY";
		protected const string __ASELLPRICE1_MDEMMS                           = "aSellPrice1_MDEMMS";		
		protected const string __ABUYPRICE2                                   = "aBuyPrice2";
		protected const string __ABUYQUANTITY2                                = "aBuyQuantity2";
		protected const string __ABUYPRICE2_NOO                               = "aBuyPrice2_NOO";
		protected const string __ABUYPRICE2_MDEY                              = "aBuyPrice2_MDEY";
		protected const string __ABUYPRICE2_MDEMMS                            = "aBuyPrice2_MDEMMS";
		protected const string __ASELLPRICE2                                  = "aSellPrice2";
		protected const string __ASELLQUANTITY2                               = "aSellQuantity2";
		protected const string __ASELLPRICE2_NOO                              = "aSellPrice2_NOO";
		protected const string __ASELLPRICE2_MDEY                             = "aSellPrice2_MDEY";
		protected const string __ASELLPRICE2_MDEMMS                           = "aSellPrice2_MDEMMS";
		protected const string __ABUYPRICE3                                   = "aBuyPrice3";		
		protected const string __ABUYQUANTITY3                                = "aBuyQuantity3";
		protected const string __ABUYPRICE3_NOO                               = "aBuyPrice3_NOO";
		protected const string __ABUYPRICE3_MDEY                              = "aBuyPrice3_MDEY";
		protected const string __ABUYPRICE3_MDEMMS                            = "aBuyPrice3_MDEMMS";
		protected const string __ASELLPRICE3                                  = "aSellPrice3";
		protected const string __ASELLQUANTITY3                               = "aSellQuantity3";
		protected const string __ASELLPRICE3_NOO                              = "aSellPrice3_NOO";
		protected const string __ASELLPRICE3_MDEY                             = "aSellPrice3_MDEY";
		protected const string __ASELLPRICE3_MDEMMS                           = "aSellPrice3_MDEMMS";	        
	    protected const string __ABUYPRICE4                                   = "aBuyPrice4";
		protected const string __ABUYQUANTITY4                                = "aBuyQuantity4";
		protected const string __ABUYPRICE4_NOO                               = "aBuyPrice4_NOO";
		protected const string __ABUYPRICE4_MDEY                              = "aBuyPrice4_MDEY";
		protected const string __ABUYPRICE4_MDEMMS                            = "aBuyPrice4_MDEMMS";
		protected const string __ASELLPRICE4                                  = "aSellPrice4";
		protected const string __ASELLQUANTITY4                               = "aSellQuantity4";
		protected const string __ASELLPRICE4_NOO                              = "aSellPrice4_NOO";
		protected const string __ASELLPRICE4_MDEY                             = "aSellPrice4_MDEY";
		protected const string __ASELLPRICE4_MDEMMS                           = "aSellPrice4_MDEMMS";	
		protected const string __ABUYPRICE5                                   = "aBuyPrice5";
		protected const string __ABUYQUANTITY5                                = "aBuyQuantity5";
		protected const string __ABUYPRICE5_NOO                               = "aBuyPrice5_NOO";
		protected const string __ABUYPRICE5_MDEY                              = "aBuyPrice5_MDEY";
		protected const string __ABUYPRICE5_MDEMMS                            = "aBuyPrice5_MDEMMS";
		protected const string __ASELLPRICE5                                  = "aSellPrice5";
		protected const string __ASELLQUANTITY5                               = "aSellQuantity5";
		protected const string __ASELLPRICE5_NOO                              = "aSellPrice5_NOO";
		protected const string __ASELLPRICE5_MDEY                             = "aSellPrice5_MDEY";
		protected const string __ASELLPRICE5_MDEMMS                           = "aSellPrice5_MDEMMS";
		protected const string __ABUYPRICE6                                   = "aBuyPrice6";		
		protected const string __ABUYQUANTITY6                                = "aBuyQuantity6";
		protected const string __ABUYPRICE6_NOO                               = "aBuyPrice6_NOO";
		protected const string __ABUYPRICE6_MDEY                              = "aBuyPrice6_MDEY";
		protected const string __ABUYPRICE6_MDEMMS                            = "aBuyPrice6_MDEMMS";
		protected const string __ASELLPRICE6                                  = "aSellPrice6";
		protected const string __ASELLQUANTITY6                               = "aSellQuantity6";
		protected const string __ASELLPRICE6_NOO                              = "aSellPrice6_NOO";
		protected const string __ASELLPRICE6_MDEY                             = "aSellPrice6_MDEY";
		protected const string __ASELLPRICE6_MDEMMS                           = "aSellPrice6_MDEMMS";
		protected const string __ABUYPRICE7                                   = "aBuyPrice7";		
		protected const string __ABUYQUANTITY7                                = "aBuyQuantity7";
		protected const string __ABUYPRICE7_NOO                               = "aBuyPrice7_NOO";
		protected const string __ABUYPRICE7_MDEY                              = "aBuyPrice7_MDEY";
		protected const string __ABUYPRICE7_MDEMMS                            = "aBuyPrice7_MDEMMS";
		protected const string __ASELLPRICE7                                  = "aSellPrice7";
		protected const string __ASELLQUANTITY7                               = "aSellQuantity7";
		protected const string __ASELLPRICE7_NOO                              = "aSellPrice7_NOO";
		protected const string __ASELLPRICE7_MDEY                             = "aSellPrice7_MDEY";
		protected const string __ASELLPRICE7_MDEMMS                           = "aSellPrice7_MDEMMS";		
		protected const string __ABUYPRICE8                                   = "aBuyPrice8";
		protected const string __ABUYQUANTITY8                                = "aBuyQuantity8";
		protected const string __ABUYPRICE8_NOO                               = "aBuyPrice8_NOO";
		protected const string __ABUYPRICE8_MDEY                              = "aBuyPrice8_MDEY";
		protected const string __ABUYPRICE8_MDEMMS                            = "aBuyPrice8_MDEMMS";
		protected const string __ASELLPRICE8                                  = "aSellPrice8";
		protected const string __ASELLQUANTITY8                               = "aSellQuantity8";
		protected const string __ASELLPRICE8_NOO                              = "aSellPrice8_NOO";
		protected const string __ASELLPRICE8_MDEY                             = "aSellPrice8_MDEY";
		protected const string __ASELLPRICE8_MDEMMS                           = "aSellPrice8_MDEMMS";	
		protected const string __ABUYPRICE9                                   = "aBuyPrice9";
		protected const string __ABUYQUANTITY9                                = "aBuyQuantity9";
		protected const string __ABUYPRICE9_NOO                               = "aBuyPrice9_NOO";
		protected const string __ABUYPRICE9_MDEY                              = "aBuyPrice9_MDEY";
		protected const string __ABUYPRICE9_MDEMMS                            = "aBuyPrice9_MDEMMS";
		protected const string __ASELLPRICE9                                  = "aSellPrice9";
		protected const string __ASELLQUANTITY9                               = "aSellQuantity9";
		protected const string __ASELLPRICE9_NOO                              = "aSellPrice9_NOO";
		protected const string __ASELLPRICE9_MDEY                             = "aSellPrice9_MDEY";
		protected const string __ASELLPRICE9_MDEMMS                           = "aSellPrice9_MDEMMS";
		protected const string __ABUYPRICE10                                  = "aBuyPrice10";
		protected const string __ABUYQUANTITY10                               = "aBuyQuantity10";
		protected const string __ABUYPRICE10_NOO                              = "aBuyPrice10_NOO";
		protected const string __ABUYPRICE10_MDEY                             = "aBuyPrice10_MDEY";
		protected const string __ABUYPRICE10_MDEMMS                           = "aBuyPrice10_MDEMMS";
		protected const string __ASELLPRICE10                                 = "aSellPrice10";
		protected const string __ASELLQUANTITY10                              = "aSellQuantity10";
		protected const string __ASELLPRICE10_NOO                             = "aSellPrice10_NOO";
		protected const string __ASELLPRICE10_MDEY                            = "aSellPrice10_MDEY";
		protected const string __ASELLPRICE10_MDEMMS                          = "aSellPrice10_MDEMMS";
		protected const string __AMATCHPRICE                                  = "aMatchPrice";
		protected const string __AMATCHQUANTITY                               = "aMatchQuantity";
		protected const string __AOPENPRICE                                   = "aOpenPrice";
		protected const string __ACLOSEPRICE                                  = "aClosePrice";
		protected const string __AHIGHESTPRICE                                = "aHighestPrice";
		protected const string __ALOWESTPRICE                                 = "aLowestPrice";
		protected const string __REPEATINGDATAFIX                             = "RepeatingDataFix";
		protected const string __REPEATINGDATAJSON                            = "RepeatingDataJson";
		protected const string __ATRANSDATE                                   = "aTransDate";
		protected const string __AOPNPX                                       = "aOpnPx";
		protected const string __ATRDSESSNHIGHPX                              = "aTrdSessnHighPx";
		protected const string __ATRDSESSNLOWPX                               = "aTrdSessnLowPx";
		protected const string __AOPNPXYLD                                    = "aOpnPxYld";
		protected const string __ATRDSESSNHIGHPXYLD                           = "aTrdSessnHighPxYld";
		protected const string __ATRDSESSNLOWPXYLD                            = "aTrdSessnLowPxYld";
		protected const string __ACLSPXYLD                                    = "aClsPxYld";


		//Msg D thêm 2 trường
        protected const string __ASECTORTYPECODE = "asectortypecode";
        protected const string __AREDUMPTIONDATE = "aredumptiondate";

        //Msg f thêm 2 trường
        protected const string __ATSCPRODGRPID = "aTscProdGrpId";
        protected const string __AHALTRSNCODE = "aHaltRsnCode";

        protected const string __RETURNCODE = "ReturnCode";
		protected const string __RETURNMESS = "ReturnMess";

        //var
       
             
        protected const string __PID = "PID";
        protected const string __PSYMBOL = "PSYMBOL";
        protected const string __PBOARDCODE = "PBOARDCODE";
        protected const string __PSECURITYTYPE = "PSECURITYTYPE";
        protected const string __PBASICPRICE = "PBASICPRICE";
        protected const string __PMATCHPRICE = "PMATCHPRICE";
        protected const string __POPENPRICE = "POPENPRICE";
        protected const string __PCLOSEPRICE = "PCLOSEPRICE";
        protected const string __PMIDPX = "PMIDPX";
        protected const string __PHIGHESTPRICE = "PHIGHESTPRICE";
        protected const string __PLOWESTPRICE = "PLOWESTPRICE";
        protected const string __PNM_TOTALTRADEDQTTY = "PNM_TOTALTRADEDQTTY";

        // var
        protected readonly IS6GApp _cS6GApp;
		protected readonly EPriceConfig _ePriceConfig;

		/// <summary>
		/// constructor
		/// </summary>
		/// <param name="s6GApp"></param>
		public CBasePrice(IS6GApp s6GApp, EPriceConfig ePriceConfig)
		{
			this._cS6GApp = s6GApp;
			this._ePriceConfig = ePriceConfig;
		}

		/// <summary>
		/// destructor
		/// </summary>
		~CBasePrice()
		{

		}
	}
}
