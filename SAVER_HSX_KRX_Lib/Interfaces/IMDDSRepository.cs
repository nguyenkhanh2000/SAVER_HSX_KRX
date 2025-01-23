using MDDSCore.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SystemCore.Entities;

namespace BaseSaverLib.Interfaces
{
    public interface IMDDSRepository
    {
        Task<EBulkScript> GetScriptSecurityDefinition(ESecurityDefinition eSD);
        Task<EBulkScript> GetScriptSecurityStatus(ESecurityStatus eSS);
        Task<EBulkScript> GetScriptSecurityInformationNotification(ESecurityInformationNotification eSIN);
        Task<EBulkScript> GetScriptSymbolClosingInformation(ESymbolClosingInformation eSCI);
        Task<EBulkScript> GetScriptVolatilityInterruption(EVolatilityInterruption eVI);
        Task<EBulkScript> GetScriptMarketMakerInformation(EMarketMakerInformation eMMI);
        Task<EBulkScript> GetScriptSymbolEvent(ESymbolEvent eSE);
        Task<EBulkScript> GetScriptIndexConstituentsInformation(EIndexConstituentsInformation eICI);
        Task<EBulkScript> GetScriptRandomEnd(ERandomEnd eRE);
        Task<EBulkScript> GetScriptIndex(EIndex eI);
        Task<EBulkScript> GetScriptInvestorperIndustry(EInvestorPerIndustry eIPI);
        //Task<EBulkScript> GetScriptInvestorperIndustryBond(EInvestorPerIndustryBond eIPIB);
        //Task<EBulkScript> GetScriptInvestorperIndustryDerivatives(EInvestorPerIndustryDerivatives eIPID);
        Task<EBulkScript> GetScriptInvestorperSymbol(EInvestorPerSymbol eIPS);
        Task<EBulkScript> GetScriptTopNMembersperSymbol(ETopNMembersPerSymbol eTNMPS);
        Task<EBulkScript> GetScriptOpenInterest(EOpenInterest eOI);
        Task<EBulkScript> GetScriptDeemTradePrice(EDeemTradePrice eDTP);
        Task<EBulkScript> GetScriptForeignerOrderLimit(EForeignerOrderLimit eFOL);
        Task<EBulkScript> GetScriptPriceLimitExpansion(EPriceLimitExpansion ePLE);
        Task<EBulkScript> GetScriptETFiNav(EETFiNav EiN);
        Task<EBulkScript> GetScriptETFiIndex(EETFiIndex EiI);
        Task<EBulkScript> GetScriptETFTrackingError(EETFTrackingError ETE);
        Task<EBulkScript> GetScriptTopNSymbolswithTradingQuantity(ETopNSymbolsWithTradingQuantity ETNSWTQ);
        Task<EBulkScript> GetScriptTopNSymbolswithCurrentPrice(ETopNSymbolsWithCurrentPrice ETNSWCP);
        Task<EBulkScript> GetScriptTopNSymbolswithHighRatioofPrice(ETopNSymbolsWithHighRatioOfPrice ETNSWHROP);
        Task<EBulkScript> GetScriptTopNSymbolswithLowRatioofPrice(ETopNSymbolsWithLowRatioOfPrice ETNSWLROP);
        Task<EBulkScript> GetScriptTradingResultofForeignInvestors(ETradingResultOfForeignInvestors ETRFI);
        Task<EBulkScript> GetScriptDisclosure(EDisclosure eD);
        Task<EBulkScript> GetScriptTimeStampPolling(ETimeStampPolling eTSP);
        Task<EBulkScript> GetScriptPriceAll(EPrice eP);
        Task<EBulkScript> GetScriptPriceRecoveryAll(EPriceRecovery ePR);
        Task<EDalResult> ExecBulkScript(string mssqlScript, string oracleScript);
        Task<EDalResult> ExecBulkScript(string mssqlScript);

        Task<EBulkScript> GetScriptDrvProductEvent(EDrvProductEvent eDRV);
    }
}

