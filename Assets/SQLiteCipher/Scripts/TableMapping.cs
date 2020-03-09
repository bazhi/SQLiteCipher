using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using SQLite.Attribute;
using Sqlite3DatabaseHandle = System.IntPtr;
using Sqlite3Statement = System.IntPtr;

namespace SQLiteCipher
{
    public class TableMapping
    {
        private readonly Column _autoPk;
        private Column[] _insertColumns;

        private PreparedSqliteInsertCommand _insertCommand;
        private string _insertCommandExtra;
        private Column[] _insertOrReplaceColumns;

        public TableMapping(Type type, CreateFlags createFlags = CreateFlags.None)
        {
            this.MappedType = type;
            TableAttribute tableAttr =
                (TableAttribute)type.GetCustomAttributes(typeof(TableAttribute), true).FirstOrDefault();
            this.TableName = tableAttr != null ? tableAttr.Name : this.MappedType.Name;

            PropertyInfo[] props = this.MappedType.GetProperties(BindingFlags.Public | BindingFlags.Instance |
                                                                 BindingFlags.SetProperty);
            List<Column> cols = new List<Column>();
            foreach (PropertyInfo p in props) {
                bool ignore = p.GetCustomAttributes(typeof(IgnoreAttribute), true).Length > 0;
                if (p.CanWrite && !ignore) cols.Add(new Column(p, createFlags));
            }

            this.Columns = cols.ToArray();
            foreach (Column c in this.Columns) {
                if (c.IsAutoInc && c.IsPK) _autoPk = c;
                if (c.IsPK) this.PK = c;
            }

            this.HasAutoIncPK = _autoPk != null;

            if (this.PK != null)
                this.GetByPrimaryKeySQL =
                    string.Format("select * from \"{0}\" where \"{1}\" = ?", this.TableName, this.PK.Name);
            else
                this.GetByPrimaryKeySQL = string.Format("select * from \"{0}\" limit 1", this.TableName);
        }

        public Type MappedType { get; private set; }
        public string TableName { get; private set; }
        public Column[] Columns { get; private set; }
        public Column PK { get; private set; }
        public string GetByPrimaryKeySQL { get; private set; }

        public bool HasAutoIncPK { get; private set; }

        public Column[] InsertColumns {
            get {
                if (_insertColumns == null) _insertColumns = this.Columns.Where(c => !c.IsAutoInc).ToArray();
                return _insertColumns;
            }
        }

        public Column[] InsertOrReplaceColumns {
            get {
                if (_insertOrReplaceColumns == null) _insertOrReplaceColumns = this.Columns.ToArray();
                return _insertOrReplaceColumns;
            }
        }

        public void SetAutoIncPK(object obj, long id)
        {
            if (_autoPk != null) _autoPk.SetValue(obj, Convert.ChangeType(id, _autoPk.ColumnType, null));
        }

        public Column FindColumnWithPropertyName(string propertyName)
        {
            Column exact = this.Columns.FirstOrDefault(c => c.PropertyName == propertyName);
            return exact;
        }

        public Column FindColumn(string columnName)
        {
            Column exact = this.Columns.FirstOrDefault(c => c.Name == columnName);
            return exact;
        }

        public PreparedSqliteInsertCommand GetInsertCommand(SQLiteConnection conn, string extra)
        {
            if (_insertCommand == null) {
                _insertCommand = CreateInsertCommand(conn, extra);
                _insertCommandExtra = extra;
            } else if (_insertCommandExtra != extra) {
                _insertCommand.Dispose();
                _insertCommand = CreateInsertCommand(conn, extra);
                _insertCommandExtra = extra;
            }

            return _insertCommand;
        }

        private PreparedSqliteInsertCommand CreateInsertCommand(SQLiteConnection conn, string extra)
        {
            Column[] cols = this.InsertColumns;
            string insertSql;
            if (!cols.Any() && this.Columns.Count() == 1 && this.Columns[0].IsAutoInc) {
                insertSql = string.Format("insert {1} into \"{0}\" default values", this.TableName, extra);
            } else {
                bool replacing = string.Compare(extra, "OR REPLACE", StringComparison.OrdinalIgnoreCase) == 0;

                if (replacing) cols = this.InsertOrReplaceColumns;

                insertSql = string.Format("insert {3} into \"{0}\"({1}) values ({2})", this.TableName,
                    string.Join(",", (from c in cols select "\"" + c.Name + "\"").ToArray()),
                    string.Join(",", (from c in cols select "?").ToArray()), extra);
            }

            PreparedSqliteInsertCommand insertCommand = new PreparedSqliteInsertCommand(conn);
            insertCommand.CommandText = insertSql;
            return insertCommand;
        }

        protected internal void Dispose()
        {
            if (_insertCommand != null) {
                _insertCommand.Dispose();
                _insertCommand = null;
            }
        }

        public class Column
        {
            private readonly PropertyInfo _prop;

            public Column(PropertyInfo prop, CreateFlags createFlags = CreateFlags.None)
            {
                ColumnAttribute colAttr =
                    (ColumnAttribute)prop.GetCustomAttributes(typeof(ColumnAttribute), true).FirstOrDefault();

                _prop = prop;
                this.Name = colAttr == null ? prop.Name : colAttr.Name;
                //If this type is Nullable<T> then Nullable.GetUnderlyingType returns the T, otherwise it returns null, so get the actual type instead
                this.ColumnType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                this.Collation = Orm.Collation(prop);

                this.IsPK = Orm.IsPK(prop) ||
                            (createFlags & CreateFlags.ImplicitPK) == CreateFlags.ImplicitPK &&
                            string.Compare(prop.Name, Orm.ImplicitPkName, StringComparison.OrdinalIgnoreCase) == 0;

                bool isAuto = Orm.IsAutoInc(prop) ||
                              this.IsPK && (createFlags & CreateFlags.AutoIncPK) == CreateFlags.AutoIncPK;
                this.IsAutoGuid = isAuto && this.ColumnType == typeof(Guid);
                this.IsAutoInc = isAuto && !this.IsAutoGuid;

                this.Indices = Orm.GetIndices(prop);
                if (!this.Indices.Any() &&
                    !this.IsPK &&
                    (createFlags & CreateFlags.ImplicitIndex) == CreateFlags.ImplicitIndex &&
                    this.Name.EndsWith(Orm.ImplicitIndexSuffix, StringComparison.OrdinalIgnoreCase)
                )
                    this.Indices = new[] { new IndexedAttribute() };
                this.IsNullable = !(this.IsPK || Orm.IsMarkedNotNull(prop));
                this.MaxStringLength = Orm.MaxStringLength(prop);
            }

            public string Name { get; private set; }

            public string PropertyName {
                get { return _prop.Name; }
            }

            public Type ColumnType { get; private set; }

            public string Collation { get; private set; }

            public bool IsAutoInc { get; private set; }
            public bool IsAutoGuid { get; private set; }

            public bool IsPK { get; private set; }

            public IEnumerable<IndexedAttribute> Indices { get; set; }

            public bool IsNullable { get; private set; }

            public int? MaxStringLength { get; private set; }

            public void SetValue(object obj, object val)
            {
                _prop.SetValue(obj, val, null);
            }

            public object GetValue(object obj)
            {
                return _prop.GetValue(obj, null);
            }
        }
    }
}