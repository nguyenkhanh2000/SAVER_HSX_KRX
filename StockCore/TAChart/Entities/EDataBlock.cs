using Newtonsoft.Json;
using StockCore.TAChart.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace StockCore.TAChart.Entities
{
    /// <summary>
    /// 2019-01-11 13:20:26 ngocta2
    /// Only members marked with Newtonsoft.Json.JsonPropertyAttribute or 
    /// System.Runtime.Serialization.DataMemberAttribute are serialized
    /// </summary>
    [JsonObject(MemberSerialization.OptIn)]
    public class EDataBlock
    {
        /// <summary>
        /// time
        /// </summary>
        [JsonProperty(PropertyName = "t", Order = 1)]
		public string Time { get; set; }

        /// <summary>
        /// OpenPrice
        /// </summary>
        [JsonProperty(PropertyName = "o", Order = 2)]
        public double Open { get; set; }

        /// <summary>
        /// HighestPrice
        /// </summary>
        [JsonProperty(PropertyName = "h", Order = 3)]
        public double Highest { get; set; }

        /// <summary>
        /// LowestPrice
        /// </summary>
        [JsonProperty(PropertyName = "l", Order = 4)]
        public double Lowest { get; set; }

        /// <summary>
        /// ClosePrice
        /// </summary>
        [JsonProperty(PropertyName = "c", Order = 5)]
        public double Close { get; set; }

        /// <summary>
        /// Volume
        /// </summary>
        [JsonProperty(PropertyName = "v", Order = 6)]
        public long Volume { get; set; }

        /// <summary>
        /// ko serialized field nay: 1 phut, 5 phut .....
        /// </summary>
        public DataBlockTypes BlockType {get;set;}

		/// <summary>
		/// ko serialized field nay: loai chung khoan
		/// </summary>
		public StockTypes StockType { get; set; }
		
		/// <summary>
		/// constructor
		/// </summary>
		public EDataBlock()
        {

        }

        /// <summary>
        /// constructor
        /// </summary>
        public EDataBlock (string time, double open, double highest, double lowest, double close, long volume, DataBlockTypes blockType, StockTypes stockType)
        {
            this.Time      = time;
            this.Open      = open;
            this.Highest   = highest;
            this.Lowest    = lowest;
            this.Close     = close;
            this.Volume    = volume;
            this.BlockType = blockType;
			this.StockType = stockType;
		}
    }
}
