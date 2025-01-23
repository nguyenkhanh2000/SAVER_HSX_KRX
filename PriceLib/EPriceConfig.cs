using System;

namespace PriceLib
{
	public class EPriceConfig
	{
		public const string __LOG_SQL_FILENAME = "PriceLib.js";
		public const string __SECTION_PRICECONFIG = "PriceConfig";		

		/// <summary>
		/// "Server=10.26.2.33; Database=STCADAPTER;User Id=Chart3APIService;Password=Chart3MakeHist@2019; MultipleActiveResultSets=true",
		/// </summary>
		public string ConnectionMssql { get; set; }

		/// <summary>
		/// "Data Source=tradingtest;User Id=authen;Password=authen1234"
		/// </summary>
		public string ConnectionOracle { get; set; }
      //  public string ConnectionOracle2 { get; set; }

        /// <summary>
        /// MDDS.dbo.prc_S6G_PS_UpdateSecurityDefinition
        /// </summary>
        public string SpMddsMssqlUpdateSecurityDefinition { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdatetSecurityStatus
		/// </summary>
		public string SpMddsMssqlUpdateSecurityStatus { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdatetSecurityInformationNotification
		/// </summary>
		public string SpMddsMssqlUpdateSecurityInformationNotification { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdatetSymbolClosingInformation
		/// </summary>
		public string SpMddsMssqlUpdateSymbolClosingInformation { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateVolatilityInterruption
		/// </summary>
		public string SpMddsMssqlUpdateVolatilityInterruption { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateMarketMakerInformation
		/// </summary>
		public string SpMddsMssqlUpdateMarketMakerInformation { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateSymbolEvent
		/// </summary>
		public string SpMddsMssqlUpdateSymbolEvent { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateIndexConstituentsInformation
		/// </summary>
		public string SpMddsMssqlUpdateIndexConstituentsInformation { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateRandomEnd
		/// </summary>
		public string SpMddsMssqlUpdateRandomEnd { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdatePriceAll
		/// </summary>
		public string SpMddsMssqlUpdatePriceAll { get; set; }


		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdatePriceRepeat
		/// </summary>
		public string SpMddsMssqlUpdatePriceRepeat { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateInvestorperIndustry
		/// </summary>
		public string SpMddsMssqlUpdateInvestorperIndustry { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateInvestorperSymbol
		/// </summary>
		public string SpMddsMssqlUpdateInvestorperSymbol { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdatePriceLimitExpansion
		/// </summary>
		public string SpMddsMssqlUpdatePriceLimitExpansion { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateIndex
		/// </summary>
		public string SpMddsMssqlUpdateIndex { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateTopNMembersperSymbol
		/// </summary>
		public string SpMddsMssqlUpdateTopNMembersperSymbol { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateOpenInterest
		/// </summary>
		public string SpMddsMssqlUpdateOpenInterest { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateDeemTradePrice
		/// </summary>
		public string SpMddsMssqlUpdateDeemTradePrice { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateForeignerOrderLimit
		/// </summary>
		public string SpMddsMssqlUpdateForeignerOrderLimit { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateETFiNav
		/// </summary>
		public string SpMddsMssqlUpdateETFiNav { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateETFiIndex
		/// </summary>
		public string SpMddsMssqlUpdateETFiIndex { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateETFTrackingError
		/// </summary>
		public string SpMddsMssqlUpdateETFTrackingError { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateTopNSymbolsWTradingQuantity
		/// </summary>
		public string SpMddsMssqlUpdateTopNSymbolswithTradingQuantity { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateTopNSymbolsWCurrentPrice
		/// </summary>
		public string SpMddsMssqlUpdateTopNSymbolswithCurrentPrice { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateTopNSymbolsWHighRatioOPrice
		/// </summary>
		public string SpMddsMssqlUpdateTopNSymbolswithHighRatioofPrice { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateTopNSymbolsWLowRatioOPrice
		/// </summary>
		public string SpMddsMssqlUpdateTopNSymbolswithLowRatioofPrice { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateTradingResultofForeignInvestors
		/// </summary>
		public string SpMddsMssqlUpdateTradingResultofForeignInvestors { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateDisclosure
		/// </summary>
		public string SpMddsMssqlUpdateDisclosure { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdateTimeStampPolling
		/// </summary>
		public string SpMddsMssqlUpdateTimeStampPolling { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdatePriceIntraday
		/// </summary>
		public string SpMddsMssqlUpdatePriceIntraday { get; set; }

		/// <summary>
		/// MDDS.dbo.prc_S6G_PS_UpdatePriceRecovery
		/// </summary>
		public string SpMddsMssqlUpdatePriceRecoveryAll { get; set; }

		/// <summary>
		/// s6g.sp_tsecuritydefinition_insert
		/// </summary>
		public string SpMddsOracleUpdateSecurityDefinition { get; set; }

		/// <summary>
		/// s6g.sp_tsecuritystatus_insert
		/// </summary>
		public string SpMddsOracleUpdateSecurityStatus { get; set; }

		/// <summary>
		/// s6g.sp_tsinformationnoti_insert
		/// </summary>
		public string SpMddsOracleUpdateSecurityInformationNotification { get; set; }

		/// <summary>
		/// s6g.sp_tsymbolclosinginfor_insert
		/// </summary>
		public string SpMddsOracleUpdateSymbolClosingInformation { get; set; }

		/// <summary>
		/// s6g.sp_tvolatilityinter_insert
		/// </summary>
		public string SpMddsOracleUpdateVolatilityInterruption { get; set; }

		/// <summary>
		/// s6g.sp_tmarketmakerinfor_insert
		/// </summary>
		public string SpMddsOracleUpdateMarketMakerInformation { get; set; }

		/// <summary>
		/// s6g.sp_tmarketmakerinfor_insert
		/// </summary>
		public string SpMddsOracleUpdateSymbolEvent { get; set; }

		/// <summary>
		/// s6g.sp_tindexconstituentsi_insert
		/// </summary>
		public string SpMddsOracleUpdateIndexConstituentsInformation { get; set; }

		/// <summary>
		/// s6g.sp_trandomend_insert
		/// </summary>
		public string SpMddsOracleUpdateRandomEnd { get; set; }

		/// <summary>
		/// s6g.sp_tprice_insert
		/// </summary>
		public string SpMddsOracleUpdatePriceAll { get; set; }

		/// <summary>
		/// s6g.sp_tinvestorperindustry_insert
		/// </summary>
		public string SpMddsOracleUpdateInvestorperIndustry { get; set; }


		/// <summary>
		/// s6g.sp_tinvestorpersymbol_insert
		/// </summary>
		public string SpMddsOracleUpdateInvestorperSymbol { get; set; }

		/// <summary>
		/// s6g.sp_tindex_insert
		/// </summary>
		public string SpMddsOracleUpdateIndex { get; set; }

		/// <summary>
		/// s6g.sp_ttopnmemberspersym_insert
		/// </summary>
		public string SpMddsOracleUpdateTopNMembersperSymbol { get; set; }

		/// <summary>
		/// s6g.sp_topeninterest_insert
		/// </summary>
		public string SpMddsOracleUpdateOpenInterest { get; set; }

		/// <summary>
		/// s6g.sp_tdeemtradeprice_insert
		/// </summary>
		public string SpMddsOracleUpdateDeemTradePrice { get; set; }

		/// <summary>
		/// s6g.sp_tforeignerorderlimit_insert
		/// </summary>
		public string SpMddsOracleUpdateForeignerOrderLimit { get; set; }

		/// <summary>
		/// s6g.sp_tpricelimitexpansion_insert
		/// </summary>
		public string SpMddsOracleUpdatePriceLimitExpansion { get; set; }

		/// <summary>
		/// s6g.sp_tetfinav_insert
		/// </summary>
		public string SpMddsOracleUpdateETFiNav { get; set; }

		/// <summary>
		/// s6g.sp_etfiindex_insert
		/// </summary>
		public string SpMddsOracleUpdateETFiIndex { get; set; }

		/// <summary>
		/// s6g.sp_tetftrackingerror_insert
		/// </summary>
		public string SpMddsOracleUpdateETFTrackingError { get; set; }

		/// <summary>
		/// s6g.sp_ttopnsymbolswtradq_insert
		/// </summary>
		public string SpMddsOracleUpdateTopNSymbolswithTradingQuantity { get; set; }

		/// <summary>
		/// s6g.sp_topnsymbolswcurrentp_insert
		/// </summary>
		public string SpMddsOracleUpdateTopNSymbolswithCurrentPrice { get; set; }

		/// <summary>
		/// s6g.sp_ttopnsymbolswhighrop_insert
		/// </summary>
		public string SpMddsOracleUpdateTopNSymbolswithHighRatioofPrice { get; set; }

		/// <summary>
		/// s6g.sp_ttopnsymbolswlowrop_insert
		/// </summary>
		public string SpMddsOracleUpdateTopNSymbolswithLowRatioofPrice { get; set; }

		/// <summary>
		/// s6g.sp_ttradingresultoffi_insert
		/// </summary>
		public string SpMddsOracleUpdateTradingResultofForeignInvestors { get; set; }

		/// <summary>
		/// s6g.sp_tdisclosure_insert
		/// </summary>
		public string SpMddsOracleUpdateDisclosure { get; set; }

		/// <summary>
		/// s6g.sp_ttimestamppolling_insert
		/// </summary>
		public string SpMddsOracleUpdateTimeStampPolling { get; set; }


		/// <summary>
		/// s6g.sp_tpricerecoverybond_insert
		/// </summary>
		public string SpMddsOracleUpdatePriceRecoveryAll { get; set; }

		//SP insert oracle tbl_ig3_si
        public string SpMddsOracleUpdatePriceIG3SI { get; set; }

        //SP insert sql tDrvProductEvent
        public string SpMddsMssqlEDrvProductEvent { get; set; }




    }
}
