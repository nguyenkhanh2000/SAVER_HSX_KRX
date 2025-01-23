namespace StockCore.Stock6G.Entities
{
    public class EPostMarketWatch
    {
        public string Name { get; set; }
        public string ClientCode { get; set; }

    }
    public class EDeleteMarketWatch
    {
        public long Score { get; set; }
        public string ClientCode { get; set; }

    }

    public class EPutMarketWatch : EDeleteMarketWatch
    {
        public string Symbols { get; set; }
        public int Position { get; set; }
        public string Type { get; set; }
        public int Row { get; set; }

    }

    public class EMarketWatch
    {

        public long Score { get; set; }
        public string ClientCode { get; set; }
        public EMarketWatchData Data { get; set; }

    }
    
    public class EMarketWatchData
    {
       
        public string Name { get; set; }
       
        public string List { get; set; }
        
        public string Row { get; set; }
       
        public string Score { get; set; }
        public string Default_MarketWatch { get; set; }

    }
}
