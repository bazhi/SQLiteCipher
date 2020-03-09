using System;
using System.Diagnostics;
using Sqlite3DatabaseHandle = System.IntPtr;
using Sqlite3Statement = System.IntPtr;

namespace SQLiteCipher
{
    public class PreparedSqliteInsertCommand : IDisposable
    {
        internal static readonly Sqlite3Statement NullStatement = default(Sqlite3Statement);

        internal PreparedSqliteInsertCommand(SQLiteConnection conn)
        {
            this.Connection = conn;
        }

        public bool Initialized { get; set; }
        public string CommandText { get; set; }

        protected SQLiteConnection Connection { get; set; }
        protected Sqlite3Statement Statement { get; set; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public int ExecuteNonQuery(object[] source)
        {
            if (this.Connection.Trace) Debug.WriteLine("Executing: " + this.CommandText);

            SQLite3.Result r = SQLite3.Result.OK;

            if (!this.Initialized) {
                this.Statement = Prepare();
                this.Initialized = true;
            }

            //bind the values.
            if (source != null)
                for (int i = 0; i < source.Length; i++)
                    SQLiteCommand.BindParameter(this.Statement, i + 1, source[i], this.Connection.StoreDateTimeAsTicks);
            r = SQLite3.Step(this.Statement);

            if (r == SQLite3.Result.Done) {
                int rowsAffected = SQLite3.Changes(this.Connection.Handle);
                SQLite3.Reset(this.Statement);
                return rowsAffected;
            }

            if (r == SQLite3.Result.Error) {
                string msg = SQLite3.GetErrmsg(this.Connection.Handle);
                SQLite3.Reset(this.Statement);
                throw SQLiteException.New(r, msg);
            }

            if (r == SQLite3.Result.Constraint &&
                SQLite3.ExtendedErrCode(this.Connection.Handle) == SQLite3.ExtendedResult.ConstraintNotNull) {
                SQLite3.Reset(this.Statement);
                throw NotNullConstraintViolationException.New(r, SQLite3.GetErrmsg(this.Connection.Handle));
            }

            SQLite3.Reset(this.Statement);
            throw SQLiteException.New(r, r.ToString());
        }

        protected virtual Sqlite3Statement Prepare()
        {
            IntPtr stmt = SQLite3.Prepare2(this.Connection.Handle, this.CommandText);
            return stmt;
        }

        private void Dispose(bool disposing)
        {
            if (this.Statement != NullStatement)
                try {
                    SQLite3.Finalize(this.Statement);
                } finally {
                    this.Statement = NullStatement;
                    this.Connection = null;
                }
        }

        ~PreparedSqliteInsertCommand()
        {
            Dispose(false);
        }
    }
}
