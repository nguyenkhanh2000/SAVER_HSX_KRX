using System;
using System.Collections.Generic;
using System.Text;

namespace SystemCore.Entities
{
    public class ERedisChannels
    {
        // sung dung file config rieng >>> D:\Source\Repos\Stock6G\HSXFeederApp\RedisChannels.json

        // const
        public const string __FILENAME = "RedisChannels.json";
        public const string __SECTION_REDISCHANNELS = "RedisChannels";
        public const string __SECTION_FEEDER2HUB = "Feeder2Hub";

        // sub class
        public class EFeeder2Hub
        {
            /// <summary>
            /// S6G_F2H_*
            /// </summary>
            public string ChannelPattern { get; set; }

            /// <summary>
            /// "HeartBeat": "S6G_HB_{Exchange}",
            /// </summary>
            public string HeartBeat { get; set; }

            /// <summary>
            /// "SnapshotQuote": "S6G_SQ_{Exchange}_{MarketID}_{BoardID}",
            /// </summary>
            public string SnapshotQuote { get; set; }

            /// <summary>
            /// "SnapshotIndex": "S6G_SI_{Exchange}",
            /// </summary>
            public string SnapshotIndex { get; set; }

            /// <summary>
            /// "SnapshotMarketStatus": "S6G_SMS_{Exchange}"
            /// </summary>
            public string SnapshotMarketStatus { get; set; }

        }

        // ---------------------------------------------------------

        /// <summary>
        /// ten phai giong het trong file config
        /// viet ngan gon RootApiService thanh RAS la ko nhan ra
        /// </summary>
        public EFeeder2Hub Feeder2Hub { get; set; }

        public ERedisChannels()
        {
            this.Feeder2Hub = new EFeeder2Hub();
        }
    }
}
