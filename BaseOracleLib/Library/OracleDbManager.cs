using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace BaseOracleLib.Library
{
    public class OracleDbManager : IDisposable
    {
        private readonly string _connectionString;
        private OracleConnection _connection;
        private readonly SemaphoreSlim _connectionLock = new(1, 1);
        private bool _disposed;
        public OracleDbManager(string connectionString)
        {
            _connectionString = connectionString;
        }
        public async Task<OracleConnection> GetConnectionAsync()
        {
            //OracleConnection connection = new OracleConnection(_connectionString);
            //await connection.OpenAsync();
            //return connection;

            await _connectionLock.WaitAsync();
            try
            {
                if (_connection == null || _connection.State != ConnectionState.Open)
                {
                    _connection?.Dispose(); //giải phóng kết nối cũ
                    _connection = new OracleConnection(_connectionString);
                    await _connection.OpenAsync();
                }
                return _connection;
            }
            finally
            {
                _connectionLock.Release();
            }

        }
        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
            }
        }
        //public void Dispose()
        //{
        //    if (_disposed) return;
        //    _disposed = true;

        //    if (_connection != null)
        //    {
        //        _connection.Dispose();
        //        _connection = null;
        //    }
        //    _connectionLock.Dispose();
        //}
    }  
}
