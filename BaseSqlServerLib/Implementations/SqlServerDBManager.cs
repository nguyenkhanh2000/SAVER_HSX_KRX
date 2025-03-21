using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BaseSqlServerLib.Implementations
{
    public class SqlServerDBManager : IDisposable
    {
        private readonly string _connectionString;
        private SqlConnection _connection;
        private readonly SemaphoreSlim _connectionLock = new(1, 1);
        private bool _disposed;

        public SqlServerDBManager(string connectionString)
        {
            _connectionString = connectionString;
        }
        public async Task<SqlConnection> GetConnectionAsync()
        {
            await _connectionLock.WaitAsync();
            try
            {
                if (_connection == null || _connection.State != ConnectionState.Open)
                {
                    _connection?.Dispose(); //giải phóng kết nối cũ
                    _connection = new SqlConnection(_connectionString);
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
                _connection?.Dispose();
                _connectionLock.Dispose();
            }
        }
    }
}
