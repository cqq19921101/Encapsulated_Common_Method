using MySql.Data.MySqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBHelper
{
    public class MySqlDB
    {
        private MySqlConnection _conn = null;
        private MySqlTransaction _st = null;
        private string _connString = "";

        public MySqlDB(IDbConnection dbConnection, IsolationLevel myIsolationLevel)
        {
            _conn = (MySqlConnection)dbConnection;
            _st = _conn.BeginTransaction(myIsolationLevel);
        }

        public MySqlDB(string ConnectionString)
        {
            _connString = ConnectionString;
            _conn = GetDBConnection();
        }
        public MySqlDB(string ConnectionString, IsolationLevel myIsolationLevel)
        {
            _connString = ConnectionString;
            _conn = GetDBConnection();
            _st = _conn.BeginTransaction(myIsolationLevel);
        }

        public MySqlDB(IsolationLevel myIsolationLevel)
        {
            _conn = GetDBConnection();
            _st = _conn.BeginTransaction(myIsolationLevel);
        }

        public void Commit()
        {
            _st.Commit();
            Close();
        }

        public void Rollback()
        {
            _st.Rollback();
            Close();
        }

        private void Close()
        {
            string SqlStat = _conn.State.ToString();
            if (SqlStat != "Closed")
            {
                try
                {
                    _conn.Close();
                }
                catch
                {
                    throw;
                }
            }
        }

        public MySqlConnection GetDBConnection()
        {
            MySqlConnection conn = new MySqlConnection(_connString);
            string SqlStat = conn.State.ToString();
            if (SqlStat == "Closed")
            {
                try
                {
                    conn.Open();
                }
                catch
                {
                    throw;
                }
            }
            return conn;
        }

        public DataTable Execute(string sql)
        {
            return Execute(sql, null);
        }

        public DataTable Execute(string sql, ArrayList opc)
        {
            DataTable dt = null;
            if (null == _conn)
                _conn = GetDBConnection();
            try
            {
                MySqlCommand cmd = new MySqlCommand(sql, _conn);
                MySqlDataAdapter da = new MySqlDataAdapter(cmd);
                if (null != _st)
                    da.SelectCommand.Transaction = _st;
                cmd.Parameters.Clear();
                if (null != opc)
                {
                    foreach (DataPara op in opc)
                    {
                        MySqlParameter mp = cmd.CreateParameter();
                        mp.DbType = op.Type;
                        mp.ParameterName = op.ParameterName;
                        mp.Value = op.Value;
                        cmd.Parameters.Add(mp);
                    }
                }
                dt = new DataTable("ResultTable");
                da.Fill(dt);
                da.Dispose();
            }
            catch
            {
                throw;
            }
            finally
            {
                if (null == _st)
                {
                    _conn.Close();
                    _conn.Dispose();
                    _conn = null;
                }
            }
            return dt;
        }

        public string ExecuteScalar(string sql)
        {
            return ExecuteScalar(sql, null);
        }

        public string ExecuteScalar(string sql, ArrayList opc)
        {
            string sRet = "";
            if (null == _conn)
                _conn = GetDBConnection();
            try
            {
                MySqlCommand cmd = new MySqlCommand(sql, _conn);
                cmd.Parameters.Clear();
                if (null != opc)
                {
                    foreach (DataPara op in opc)
                    {
                        MySqlParameter mp = cmd.CreateParameter();
                        mp.DbType = op.Type;
                        mp.ParameterName = op.ParameterName;
                        mp.Value = op.Value;
                        cmd.Parameters.Add(mp);
                    }
                }
                if (null != _st)
                    cmd.Transaction = _st;
                sRet = cmd.ExecuteScalar().ToString();
            }
            catch
            {
                throw;
            }
            finally
            {
                if (null == _st)
                {
                    _conn.Close();
                    _conn.Dispose();
                    _conn = null;
                }
            }
            return sRet;
        }

        public void ExecuteNonQuery(string sql)
        {
            ExecuteNonQuery(sql, null);
            return;
        }

        public void ExecuteNonQuery(string sql, ArrayList opc)
        {
            if (null == _conn)
                _conn = GetDBConnection();
            try
            {
                MySqlConnection conn = GetDBConnection();
                MySqlCommand cmd = new MySqlCommand(sql, conn);
                cmd.Parameters.Clear();
                if (null != opc)
                {
                    foreach (DataPara op in opc)
                    {
                        MySqlParameter mp = cmd.CreateParameter();
                        mp.DbType = op.Type;
                        mp.ParameterName = op.ParameterName;
                        mp.Value = op.Value;
                        cmd.Parameters.Add(mp);
                    }
                }
                if (null != _st)
                    cmd.Transaction = _st;
                cmd.ExecuteNonQuery();
            }
            catch
            {
                throw;
            }
            finally
            {
                if (null == _st)
                {
                    _conn.Close();
                    _conn.Dispose();
                    _conn = null;
                }
            }
            return;
        }
    }
}
