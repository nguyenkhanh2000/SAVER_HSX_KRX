using BaseSqlServerLib.Interfaces;
using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Security.Principal;
using System.Text;

namespace BaseSqlServerLib.Implementations
{
	public class CDemo : IDemo
	{
		private const string CONNECTION_STRING_DEVOPS_B08_WIN_AUTH = "data source=DEVOPS-B08;database=STCADAPTER;Integrated Security=SSPI;persist security info=True;Connection Timeout=5;";
		private const string CONNECTION_STRING_DEVOPS_B08_SQL_AUTH = @"Data Source=10.26.7.31\MSSQLSERVER2019,1435;Initial Catalog=STCADAPTER;User ID=sa;Password=fpts@123;Connection Timeout=5;";
		private const string SQL_SELECT = "SELECT TOP (1000) id,Name,Nick,Date,Age FROM [STCADAPTER].[dbo].[tbl_TEST]";
		/// <summary>
		/// 2019-11-13 09:07:28 ngocta2
		/// https://docs.microsoft.com/en-us/azure/sql-database/sql-database-connect-query-dotnet-core
		/// https://www.connectionstrings.com/sql-server/
		/// https://github.com/lilpug/ASP.NET-Core-Impersonation
		/// https://stackoverflow.com/questions/44290827/asp-net-core-identity-impersonate-specific-user
		/// </summary>
		/// <returns></returns>
		public int ConnectSqlServerUsingDomainUser()
		{
			int rowCount = 0;

			try
			{
				Console.WriteLine("=========ConnectSqlServerUsingDomainUser=========");

				// import System.Security.Principal.Windows 4.6.0
				//string username = WindowsIdentity.GetCurrent().Name; // FIT-NGOCTA2-PC\ngocta2
				//Console.WriteLine($"WindowsIdentity.GetCurrent().Name={username}");


				Console.WriteLine($"ConnString={CONNECTION_STRING_DEVOPS_B08_WIN_AUTH}");

				rowCount = AccessData(CONNECTION_STRING_DEVOPS_B08_WIN_AUTH);
			}
			catch (SqlException e)
			{
				Console.WriteLine($"SqlException={e.ToString()}");
			}
			finally
			{
				Console.WriteLine("Done");
			}

			return rowCount;			
		}

		/// <summary>
		/// 2019-11-13 16:38:52 ngocta2
		/// </summary>
		/// <returns></returns>
		public int ConnectSqlServerUsingSqlUser()
		{
			int rowCount = 0;

			try
			{
				Console.WriteLine("=========ConnectSqlServerUsingSqlUser=========");
				Console.WriteLine($"ConnString={CONNECTION_STRING_DEVOPS_B08_SQL_AUTH}");

				rowCount=AccessData(CONNECTION_STRING_DEVOPS_B08_SQL_AUTH);
			}
			catch (SqlException e)
			{
				Console.WriteLine($"SqlException={e.ToString()}");
			}
			finally
			{
				Console.WriteLine("Done");
			}

			return rowCount;
		}

		public int AccessData(string connString)
		{

			int rowCount = 0;

			try
			{
				using (SqlConnection connection = new SqlConnection(connString))
				{
					connection.Open();
					StringBuilder sb = new StringBuilder();

					sb.Append(SQL_SELECT);
					String sql = sb.ToString();

					Console.WriteLine($"sql={sql}");
					using (SqlCommand command = new SqlCommand(sql, connection))
					{
						using (SqlDataReader reader = command.ExecuteReader())
						{
							Console.WriteLine("{0} {1} {2} {3} {4}", "[ID]", "[Name]", "[Nick]", "[Date]", "[Age]");
							while (reader.Read())
							{
								rowCount++;
								Console.WriteLine($"{reader[0]} {reader[1]} {reader[2]} {reader[3]} {reader[4]}");
							}
						}
					}
				}
			}
			catch (SqlException e)
			{
				Console.WriteLine($"AccessData.SqlException={e.ToString()}");
			}
			finally
			{
				Console.WriteLine("AccessData.Done");
			}

			return rowCount;
		}

		public List<T> GetData<T>(int id)
		{
			using (SqlConnection conn = new SqlConnection(CONNECTION_STRING_DEVOPS_B08_SQL_AUTH))
			{
				conn.Open();
				return conn.Query<T>($"{SQL_SELECT} WHERE ID = @id or 1=1", new { id = id }).ToList();
			}
		}

		void test()
		{

		}//
	}
}
