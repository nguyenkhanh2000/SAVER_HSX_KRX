using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Interfaces
{
    /// <summary>
    /// 2019-01-08 10:46:11 ngocta2
    /// 2019-02-10 11:55:44 ngocta2 su dung StackExchange vi ServiceStack 5.x limit 6k request/hour
    /// </summary>
    public interface IRedisRepository: IInstance
    {
        // props
        object ConnectionMultiplexer { get; }
        object Subscriber { get; }

		// methods
		string String_Get(string key, int databaseNumber = -1);
		bool String_Remove(string key);
        bool String_Set(string key, string value, int duration);
        bool String_SetObject(string key, object value, int duration);
        bool ZSet_AddRow(string zKey, object dataObject, string symbol = null, string dateTimeInput = null);
		bool ZSet_AddRow(string zKey, string zValue, long zScore, bool checkExistThenSkip = true);

		bool ZSet_AddRows(string zKey, IDictionary<string, long> keyValuePairs);
        IDictionary<string, double> ZSet_GetRowsByRange(string zKey, long fromScore, long toScore);
        IDictionary<string, double> ZSet_GetRowsByRankDesc(string zKey, int fromRank, int toRank);
        string ZSet_GetValueWithHighestScore(string zKey);
        string ZSet_GetValueWithLowestScore(string zKey);
        long ZSet_RemoveRows(string zKey, long fromScore, long toScore);
        //bool ZSet_UpdateRow(string zKey, object oldDataObject, object newDataObject, int compareOffset = 0, int lengthOffset = 0);
		bool ZSet_UpdateRow(string zKey, string zValue, long zScore);


        bool Fast__String_Set(string key, string value, int duration);
    }
}
