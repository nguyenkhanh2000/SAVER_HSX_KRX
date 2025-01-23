using MDDSCore.Messages;
using StockCore.Stock6G.Entities;
using StockCore.Stock6G.JsonX;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace CommonLib.Interfaces
{
	public interface IHandCode
	{
		string                           Fix_Fix2Json(string fixString);
		string                           Fix_GetMsgType(string msg);
		string                           Fix_GetMsgTypes(string rawData);

        ESecurityDefinition              Fix_Fix2ESecurityDefinition(string rawData, int priceDividedBy = 1000, int priceRoundDigitsCount = 2);
		ETradingResultOfForeignInvestors Fix_Fix2ETradingResultOfForeignInvestors(string rawData);
		EForeignerOrderLimit             Fix_Fix2EForeignerOrderLimit(string rawData);
		ERandomEnd                       Fix_Fix2ERandomEnd(string rawData);
        //EDeemTradePrice                  Fix_Fix2EDeemTradePrice(string rawData);	
        EDeemTradePrice                  Fix_Fix2EDeemTradePrice(string rawData, int priceDividedBy = 1, int priceRoundDigitsCount = 2, int massDividedBy = 1);

        ETimeStampPolling Fix_Fix2ETimeStampPolling(string rawData);
        //  EPrice                           Fix_Fix2EPrice(string rawData, bool readAllTags = false, int priceDividedBy=1000, int priceRoundDigitsCount=2);
        //EPriceRecovery                   Fix_Fix2EPriceRecovery(string rawData, bool readAllTags = false);
        EPrice Fix_Fix2EPrice(string rawData, bool readAllTags = false, int priceDividedBy = 1, int priceRoundDigitsCount = 2, int massDividedBy = 1);
        // sử dụng cho HSX
        EPrice_hsx Fix_Fix2EPrice_hsx(string rawData, bool readAllTags = false, int priceDividedBy = 10, int priceRoundDigitsCount = 2, int massDividedBy = 100);
        EPriceRecovery Fix_Fix2EPriceRecovery(string rawData, bool readAllTags = false, int priceDividedBy = 1, int priceRoundDigitsCount = 2, int massDividedBy = 1);
        EIndex							 Fix_Fix2EIndex(string rawData);
		ESecurityStatus					 Fix_Fix2ESecurityStatus(string rawData);
        EIndexConstituentsInformation	 Fix_Fix2EIndexConstituentsInformation(string rawData);
        ESecurityInformationNotification Fix_Fix2SecurityInformationNoti(string rawData, int priceDividedBy = 1000, int priceRoundDigitsCount = 2);
        EOpenInterest Fix_Fix2EOpenInterest(string rawData);
		EPriceLimitExpansion Fix_Fix2EPriceLimitExpansion(string rawData, bool readAllTags = false);

        ESecurityInformationNotification Fix_Fix2ESecurityInformationNotification(string rawData, bool readAllTags = false);
        string Utf8Json_SerializeObject(object obj);
		string Utf8Json_SerializeObjectWrap(object obj);
		T Utf8Json_DeserializeObject<T>(string json);
		bool String_Compare(string s1, string s2);


		bool UpdateDicQuote(ref ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<string, EXQuote>>> quoteDic, string marketID, string boardID, EXQuote newQuote);
		bool UpdateDicBasket(ref ConcurrentDictionary<string, ConcurrentDictionary<string, List<string>>> basketDic, EIndexConstituentsInformation indexConstituentsInformation);
		bool UpdateDicML(ref ConcurrentDictionary<string, EXBasket> mlDic, EIndexConstituentsInformation indexConstituentsInformation);
		bool UpdateDicBasketMapSort(ref ConcurrentDictionary<string, ConcurrentDictionary<string, List<string>>> basketDic, ref ConcurrentDictionary<string, ESecurityDefinition> symbolDic);
		bool UpdateDicSymbol(ESecurityDefinition d, ref ConcurrentDictionary<string, ESecurityDefinition> symbolDic);
		bool UpdateDicIndex(ref ConcurrentDictionary<string, EXIndex> indexDic, string indexsTypeCode, EXIndex newIndex);
		bool UpdateDicIndexCode2Name(ref ConcurrentDictionary<string, EXIndex> indexDic, EIndexConstituentsInformation ml);

		bool GetLastIndex(ref ConcurrentDictionary<string, EXIndex> indexDic, string exchange);
		List<EXBasket> BasketDic2BasketList(ConcurrentDictionary<string, ConcurrentDictionary<string, List<string>>> basketDic, EIndexConstituentsInformation ml, ConcurrentDictionary<string, EXBasket> mlDic, List<EXBasket> predefinedBasketList);

		EXQuote UpdateXQuoteByCellSeqNum(EXQuote currentQuote, EXQuote newQuote);
		EXIndex UpdateXIndexByCellSeqNum(EXIndex currentIndex, EXIndex newIndex);

		EXQuote Convert_X2XQuote(EPrice x, ESecurityDefinition d);
		EXQuote Convert_d2XQuote(ESecurityDefinition d);
		EXQuote Convert_MW2XQuote(ERandomEnd mw, ESecurityDefinition d);
		EXQuote Convert_MT2XQuote(ETradingResultOfForeignInvestors mt, ESecurityDefinition d);
		EXQuote Convert_ME2XQuote(EDeemTradePrice me, ESecurityDefinition d);
		EXQuote Convert_MF2XQuote(EForeignerOrderLimit mf, ESecurityDefinition d, string boardID);		
		EXQuote Convert_f2XQuote(ESecurityStatus f, ESecurityDefinition d);
		//EXQuote Convert_ML2XQuote(EIndexConstituentsInformation ml);

		EXIndex Convert_M12XIndex(EIndex m1, ConcurrentDictionary<string, ELastIndex> lastIndexdic);

		List<EXQuoteS> EXQuoteList2EXQuoteSList(List<EXQuote> list);

		double ProcessPrice(string priceString, int priceDividedBy, int priceRoundDigitsCount);

        // interface thieu
        //EBasePrice Fix_Fix2EBasePrice(string rawData, bool readAllTags = false, int priceDividedBy = 1000, int priceRoundDigitsCount = 2);
        EBasePrice Fix_Fix2EBasePrice(string rawData, bool readAllTags = false, int priceDividedBy = 1, int priceRoundDigitsCount = 2, int massDividedBy = 1);
	}
}
