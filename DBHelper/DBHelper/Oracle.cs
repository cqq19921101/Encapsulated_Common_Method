using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.OracleClient;
using System.Data;
using System.Collections;

namespace DBHelper
{
    public class OracleDB
    {
        private OracleConnection _conn = null;
        private OracleTransaction _st = null;
        private string _connString = "";
        public OracleDB(IDbConnection dbConnection, IsolationLevel myIsolationLevel)
        {
            _conn = (OracleConnection)dbConnection;
            _st = _conn.BeginTransaction(myIsolationLevel);
        }

        public OracleDB(string ConnectionString)
        {
            _connString = ConnectionString;
            _conn = GetDBConnection();
        }
        public OracleDB(string ConnectionString, IsolationLevel myIsolationLevel)
        {
            _connString = ConnectionString;
            _conn = GetDBConnection();
            _st = _conn.BeginTransaction(myIsolationLevel);
        }

        public OracleDB(IsolationLevel myIsolationLevel)
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

        public OracleConnection GetDBConnection()
        {
            OracleConnection conn = new OracleConnection(_connString);
            string SqlStat = conn.State.ToString();
            if (SqlStat == "Closed")
            {
                conn.Open();
            }
            return conn;
        }

        public DataTable Execute(string sql)
        {
            return Execute(sql, new OracleParameterCollection());
        }

        public DataTable Execute(string sql, OracleParameterCollection opc)
        {
            DataTable dt = null;
            if (null == _conn)
                _conn = GetDBConnection();
            try
            {
                OracleConnection conn = GetDBConnection();
                OracleCommand cmd = new OracleCommand(sql, _conn);
                OracleDataAdapter da = new OracleDataAdapter(cmd);
                if (null != _st)
                    da.SelectCommand.Transaction = _st;
                foreach (OracleParameter op in opc)
                {
                    cmd.Parameters.Add(op.ParameterName, op.OracleType, op.Size).Value = op.Value;
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
            return ExecuteScalar(sql, new OracleParameterCollection());
        }

        public string ExecuteScalar(string sql, OracleParameterCollection opc)
        {
            string sRet = "";
            if (null == _conn)
                _conn = GetDBConnection();
            try
            {
                OracleCommand cmd = new OracleCommand(sql, _conn);
                foreach (OracleParameter op in opc)
                {
                    cmd.Parameters.Add(op.ParameterName, op.OracleType, op.Size).Value = op.Value;
                }
                if (null != _st)
                    cmd.Transaction = _st;
                sRet = cmd.ExecuteOracleScalar().ToString();
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
            ExecuteNonQuery(sql, new OracleParameterCollection());
            return;
        }

        public void ExecuteNonQuery(string sql, OracleParameterCollection opc)
        {
            if (null == _conn)
                _conn = GetDBConnection();
            try
            {
                OracleCommand cmd = new OracleCommand(sql, _conn);
                foreach (OracleParameter op in opc)
                {
                    cmd.Parameters.Add(op.ParameterName, op.OracleType, op.Size).Value = op.Value;
                }
                if (null != _st)
                    cmd.Transaction = _st;
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw ex;
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

        /// <summary>
        /// CALL SP RERUN Hashtable 
        /// </summary>
        /// <param name="procName">Procedure Name</param>
        /// <param name="opc">Oracle Parameter Collection</param>
        /// <returns>Hashtable</returns>
        public Hashtable ExecuteProc(string procName, OracleParameterCollection opc)
        {
            #region
            if (null == _conn)
                _conn = GetDBConnection();
            OracleCommand cmd;
            try
            {
                if (null == opc)
                {
                    cmd = CreateCommand(procName, null);
                }
                else
                {
                    cmd = CreateCommand(procName, opc);
                }
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
            Hashtable htReturn = new Hashtable();
            foreach (OracleParameter sp in cmd.Parameters)
            {
                htReturn.Add(sp.ParameterName, sp.Value.ToString().Trim());
            }
            return htReturn;
            #endregion
        }

        /// <summary>
        /// CALL SP RERUN DATATABLE 
        /// </summary>
        /// <param name="procName"></param>
        /// <param name="opc"></param>
        /// <param name="outputParameterName"></param>
        /// <returns></returns>
        public DataTable ExecuteProcTable(string procName, OracleParameterCollection opc)
        {
            #region
            if (null == _conn)
                _conn = GetDBConnection();
            DataSet ds = new DataSet();
            OracleDataAdapter oraAdapter = new OracleDataAdapter();
            try
            {
                if (null == opc)
                {
                    oraAdapter.SelectCommand = CreateCommand(procName, null);
                }
                else
                {
                    oraAdapter.SelectCommand = CreateCommand(procName, opc);
                }


                oraAdapter.Fill(ds);
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
            DataTable dt = ds.Tables[0];
            return dt;
            #endregion
        }

        private OracleCommand CreateCommand(string procName, OracleParameterCollection opc)
        {

            #region
            if (null == _conn)
                _conn = GetDBConnection();
            OracleCommand cmd = new OracleCommand(procName, _conn);
            cmd.CommandType = CommandType.StoredProcedure;
            try
            {


                cmd.Parameters.Clear();
                if (null != opc)
                {
                    foreach (OracleParameter op in opc)
                    {
                        OracleParameter mp = cmd.CreateParameter();
                        mp.OracleType = op.OracleType;
                        mp.ParameterName = op.ParameterName;
                        mp.Value = op.Value;
                        mp.Size = op.Size;
                        if (op.Value == null)
                        {
                            mp.Direction = ParameterDirection.Output;
                        }
                        else
                        {
                            mp.Direction = ParameterDirection.Input;
                        }

                        cmd.Parameters.Add(mp);
                    }
                }

            }
            catch
            {
                throw;
            }


            return cmd;
            #endregion
        }

    }
}
