using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using SQLite.Attribute;
using Sqlite3DatabaseHandle = System.IntPtr;
using Sqlite3Statement = System.IntPtr;

namespace SQLiteCipher
{
    public static class Orm
    {
        public const int DefaultMaxStringLength = 140;
        public const string ImplicitPkName = "Id";
        public const string ImplicitIndexSuffix = "Id";

        public static string SqlDecl(TableMapping.Column p, bool storeDateTimeAsTicks)
        {
            string decl = "\"" + p.Name + "\" " + SqlType(p, storeDateTimeAsTicks) + " ";

            if (p.IsPK) decl += "primary key ";
            if (p.IsAutoInc) decl += "autoincrement ";
            if (!p.IsNullable) decl += "not null ";
            if (!string.IsNullOrEmpty(p.Collation)) decl += "collate " + p.Collation + " ";

            return decl;
        }

        public static string SqlType(TableMapping.Column p, bool storeDateTimeAsTicks)
        {
            Type clrType = p.ColumnType;
            if (clrType == typeof(bool) || clrType == typeof(byte) || clrType == typeof(ushort) ||
                clrType == typeof(sbyte) || clrType == typeof(short) || clrType == typeof(int)) return "integer";

            if (clrType == typeof(uint) || clrType == typeof(long)) return "bigint";

            if (clrType == typeof(float) || clrType == typeof(double) || clrType == typeof(decimal)) return "float";

            if (clrType == typeof(string)) {
                int? len = p.MaxStringLength;

                if (len.HasValue)
                    return "varchar(" + len.Value + ")";

                return "varchar";
            }

            if (clrType == typeof(TimeSpan)) return "bigint";

            if (clrType == typeof(DateTime)) return storeDateTimeAsTicks ? "bigint" : "datetime";

            if (clrType == typeof(DateTimeOffset)) {
                return "bigint";
            }

            if (clrType.IsEnum) {
                return "integer";
            }

            if (clrType == typeof(byte[]))
                return "blob";
            if (clrType == typeof(Guid))
                return "varchar(36)";
            throw new NotSupportedException("Don't know about " + clrType);
        }

        public static bool IsPK(MemberInfo p)
        {
            object[] attrs = p.GetCustomAttributes(typeof(PrimaryKeyAttribute), true);
            return attrs.Length > 0;
        }

        public static string Collation(MemberInfo p)
        {
            object[] attrs = p.GetCustomAttributes(typeof(CollationAttribute), true);
            if (attrs.Length > 0)
                return ((CollationAttribute)attrs[0]).Value;
            return string.Empty;
        }

        public static bool IsAutoInc(MemberInfo p)
        {
            object[] attrs = p.GetCustomAttributes(typeof(AutoIncrementAttribute), true);
            return attrs.Length > 0;
        }

        public static IEnumerable<IndexedAttribute> GetIndices(MemberInfo p)
        {
            object[] attrs = p.GetCustomAttributes(typeof(IndexedAttribute), true);
            return attrs.Cast<IndexedAttribute>();
        }

        public static int? MaxStringLength(PropertyInfo p)
        {
            object[] attrs = p.GetCustomAttributes(typeof(MaxLengthAttribute), true);
            if (attrs.Length > 0)
                return ((MaxLengthAttribute)attrs[0]).Value;
            return null;
        }

        public static bool IsMarkedNotNull(MemberInfo p)
        {
            object[] attrs = p.GetCustomAttributes(typeof(NotNullAttribute), true);
            return attrs.Length > 0;
        }
    }
}
