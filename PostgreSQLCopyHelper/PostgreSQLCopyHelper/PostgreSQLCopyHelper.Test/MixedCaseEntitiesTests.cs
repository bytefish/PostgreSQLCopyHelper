﻿using Npgsql;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using PostgreSQLCopyHelper.Extensions;

using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgreSQLCopyHelper.Test
{
    public class MixedCaseEntitiesTests
    {
        [TestFixture]
        public class Mixed_Case_Test : TransactionalTestBase
        {            

            private int CreateTable(string schemaName = "")
            {
                string schemaString = schemaName == "" ? "" : schemaName + ".";

                var sqlStatement = string.Format(@"CREATE TABLE {0}""MixedCaseEntity""
                                    (
                                        ""Property_One"" integer,
                                        ""Property_Two"" text                
                                    );", schemaString);

                var sqlCommand = new NpgsqlCommand(sqlStatement, connection);

                return sqlCommand.ExecuteNonQuery();
            }

            private List<object[]> GetAll(string schemaName = "")
            {
                string schemaString = schemaName == "" ? "" : schemaName + ".";

                var sqlStatement = String.Format(@"SELECT * FROM {0}""MixedCaseEntity""", schemaString);
                var sqlCommand = new NpgsqlCommand(sqlStatement, connection);


                List<object[]> result = new List<object[]>();
                using (var dataReader = sqlCommand.ExecuteReader())
                {
                    while (dataReader.Read())
                    {
                        var values = new object[dataReader.FieldCount];
                        for (int i = 0; i < dataReader.FieldCount; i++)
                        {
                            values[i] = dataReader[i];
                        }
                        result.Add(values);
                    }
                }

                return result;
            }


            [Test]
            public void Test_Mixed_Case_With_Named_Schema()
            {
                var schema = "sample";
                CreateTable(schema);


                var subject = new PostgreSQLCopyHelper<MixedCaseEntity>(schema, "MixedCaseEntity")
                    .MapInteger("Property_One", x => x.Property_One)
                    .MapText("Property_Two", x => x.Property_Two);

                var t1 = new MixedCaseEntity
                {
                    Property_One = 44,
                    Property_Two = "hello everyone"
                };

                var t2 = new MixedCaseEntity
                {
                    Property_One = 89,
                    Property_Two = "Isn't it nice to write in Camel Case!"
                };

                var set = new HashSet<MixedCaseEntity> { t1, t2 };

                subject.SaveAll(connection, new [] { t1, t2 });

                var result = GetAll(schema);

                Assert.AreEqual(set.Count, result.Count);

                Assert.AreEqual(t1.Property_One, result[0].First());
                Assert.AreEqual(t1.Property_Two, result[0].Skip(1).First());

                Assert.AreEqual(t2.Property_One, result[1].First());
                Assert.AreEqual(t2.Property_Two, result[1].Skip(1).First());

            }

            [Test]
            public void Test_Mixed_Case_With_Default_Schema()
            {
                CreateTable();

                var subject = new PostgreSQLCopyHelper<MixedCaseEntity>("MixedCaseEntity")
                    .MapInteger("Property_One", x => x.Property_One)
                    .MapText("Property_Two", x => x.Property_Two);

                var t1 = new MixedCaseEntity
                {
                    Property_One = 44,
                    Property_Two = "hello everyone"
                };

                var t2 = new MixedCaseEntity
                {
                    Property_One = 89,
                    Property_Two = "Isn't it nice to write in Camel Case!"
                };

                var set = new HashSet<MixedCaseEntity> { t1, t2 };

                subject.SaveAll(connection, new[] { t1, t2 });

                var result = GetAll();

                Assert.AreEqual(set.Count, result.Count);

                Assert.AreEqual(t1.Property_One, result[0].First());
                Assert.AreEqual(t1.Property_Two, result[0].Skip(1).First());

                Assert.AreEqual(t2.Property_One, result[1].First());
                Assert.AreEqual(t2.Property_Two, result[1].Skip(1).First());

            }


        }

        


            
    }

    public class MixedCaseEntity
    {

        public int Property_One { get; set; }

        public string Property_Two { get; set; }
    }
}