using MDDSCore.Messages;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using SystemCore.Entities;

namespace PriceLib.Interfaces
{
	public interface IMDDS
	{
		//Task<EDalResult> UpdateSecurityDefinition(ESecurityDefinition  eSecurityDefinition);
		Task<EDalResult> UpdateSecurityStatus(ESecurityStatus eSecurityStatus, bool getScriptOnly= false);
		Task<EDalResult> UpdateSecurityInformationNotification(ESecurityInformationNotification eSecurityInformationNotification, bool getScriptOnly = false);
		Task<EDalResult> UpdateSymbolClosingInformation(ESymbolClosingInformation eSymbolClosingInformation, bool getScriptOnly = false);
		Task<EDalResult> UpdateVolatilityInterruption(EVolatilityInterruption eVolatilityInterruption, bool getScriptOnly = false);
		Task<EDalResult> UpdateMarketMakerInformation(EMarketMakerInformation eMarketMakerInformation, bool getScriptOnly= false);
		Task<EDalResult> UpdateSymbolEvent(ESymbolEvent eSymbolEvent, bool getScriptOnly= false);
		Task<EDalResult> UpdateIndexConstituentsInformation(EIndexConstituentsInformation eIndexConstituentsInformation, bool getScriptOnly= false);
		Task<EDalResult> UpdateRandomEnd(ERandomEnd eRandomEnd, bool getScriptOnly= false);
		Task<EDalResult> UpdateInvestorperIndustry(EInvestorPerIndustry eInvestorPerIndustry, bool getScriptOnly = false);
		Task<EDalResult> UpdateInvestorperSymbol(EInvestorPerSymbol eInvestorPerSymbol, bool getScriptOnly = false);
		Task<EDalResult> UpdateIndex(EIndex eIndex, bool getScriptOnly = false);
		Task<EDalResult> UpdateTopNMembersperSymbol(ETopNMembersPerSymbol eTopNMembersPerSymbol, bool getScriptOnly = false);
		Task<EDalResult> UpdateOpenInterest(EOpenInterest eOpenInterest, bool getScriptOnly = false);
		Task<EDalResult> UpdateDeemTradePrice(EDeemTradePrice eDeemTradePrice, bool getScriptOnly = false);
		Task<EDalResult> UpdateForeignerOrderLimit(EForeignerOrderLimit eForeignerOrderLimit, bool getScriptOnly = false);
		Task<EDalResult> UpdatePriceLimitExpansion(EPriceLimitExpansion ePriceLimitExpansion, bool getScriptOnly = false);
		Task<EDalResult> UpdateETFiNav(EETFiNav eETFiNav, bool getScriptOnly = false);
		Task<EDalResult> UpdateETFiIndex(EETFiIndex eETFiIndex, bool getScriptOnly = false);
		Task<EDalResult> UpdateETFTrackingError(EETFTrackingError eETFTrackingError, bool getScriptOnly = false);
		Task<EDalResult> UpdateTopNSymbolswithTradingQuantity(ETopNSymbolsWithTradingQuantity eTopNSymbolsWithTradingQuantity, bool getScriptOnly = false);
		Task<EDalResult> UpdateTopNSymbolswithCurrentPrice(ETopNSymbolsWithCurrentPrice eTopNSymbolsWithCurrentPrice, bool getScriptOnly = false);
		Task<EDalResult> UpdateSecurityDefinition(ESecurityDefinition eSecurityDefinition, bool getScriptOnly = false);
      //  Task<EDalResult> UpdateSecurityDefinitionIG3SI(ESecurityDefinition eSecurityDefinition, bool getScriptOnly = false);
        Task<EDalResult> UpdateTopNSymbolswithHighRatioofPrice(ETopNSymbolsWithHighRatioOfPrice eTopNSymbolsWithHighRatioOfPrice, bool getScriptOnly = false);
		Task<EDalResult> UpdateTopNSymbolswithLowRatioofPrice(ETopNSymbolsWithLowRatioOfPrice eTopNSymbolsWithLowRatioOfPrice, bool getScriptOnly = false);
		Task<EDalResult> UpdateTradingResultofForeignInvestors(ETradingResultOfForeignInvestors eTradingResultOfForeignInvestors, bool getScriptOnly = false);
		Task<EDalResult> UpdateDisclosure(EDisclosure eDisclosure, bool getScriptOnly = false);
		Task<EDalResult> UpdateTimeStampPolling(ETimeStampPolling eTimeStampPolling, bool getScriptOnly = false);
		Task<EDalResult> UpdatePriceAll(EPrice ePrice, bool getScriptOnly = false);
      //  Task<EDalResult> UpdatePriceAllIG3SI(EPrice ePrice, bool getScriptOnly = false);
        Task<EDalResult> UpdatePriceRecoveryAll(EPriceRecovery ePriceRecovery, bool getScriptOnly = false);

		Task<EDalResult> ExecuteScript(string script);



        // THÊM MSG MJ
        Task<EDalResult> UpdateDrvProductEventAll(EDrvProductEvent eDrvProductEvent, bool getScriptOnly = false);
    }
}
