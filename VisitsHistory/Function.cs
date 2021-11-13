using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using MySql.Data.MySqlClient;
using StackExchange.Redis;
using Newtonsoft.Json;
using Dapper;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace VisitsHistory
{
    public class Function
    {
        
        /// <summary>
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<IEnumerable<History>> FunctionHandler(string input, ILambdaContext context)
        {
            // if sql exists = false
                // select only from cache

            // In Lambda, Console.WriteLine goes to CloudWatch Logs.
            var task1 = Task.Run(() => GetFromDatabaseAsync(input));
            var task2 = Task.Run(() => GetFromCacheAsync(input));

            // Lambda may return before printing "Test2" since we never wait on task2.
            IEnumerable<IEnumerable<History>> History_Collection = await Task.WhenAll(task1, task2);

            var visithistory = from historyobj in History_Collection
                   from history in historyobj
                   orderby history.VisitDate descending
                   select history;

            HashSet<History> visithistoryset = new HashSet<History>();

            foreach (var item in visithistory.ToList())
            {
                visithistoryset.Add(item);
            }

            return visithistoryset;
        }

        private IEnumerable<History> GetFromDatabaseAsync(string input)
        {
            using (var connection = new MySqlConnection("Server=localhost;Database=sakila;Uid=root;Pwd=sysad;"))
            {
                IEnumerable<Name>? result = connection.Query<Name>("select a.first_name as FirstName , a.last_name as LastName from actor a where a.actor_id = 300 union select s.first_name as FirstName , s.last_name as LastName from staff s where s.staff_id = 20");
                if (result is null)
                {
                    Console.WriteLine("database returned 0 rows");
                }

                foreach (var item in result)
                {
                    Console.WriteLine($"{item.FirstName}-{item.LastName}");
                }
              
            }
            return new List<History> { new History("john cow", new DateTime(2021, 03, 02, 12, 07, 06), "my property", "COMPLETED"), new History("john cow", new DateTime(2021, 03, 02, 12, 07, 06), "my property", "COMPLETED") };
        }

        //private bool GetExists(string input)
        //{
        //    //using (var connection = new MySqlConnection("Server=localhost;Database=sakila;Uid=root;Pwd=sysad;"))
        //    //{
        //    //    IEnumerable<Name>? result = connection.Query<Name>("select a.first_name as FirstName , a.last_name as LastName from actor a where a.actor_id = 300 union select s.first_name as FirstName , s.last_name as LastName from staff s where s.staff_id = 20");
        //    //}
        //    return true;
        //}

        private async Task<IEnumerable<History>> GetFromCacheAsync(string input)
        {
            using var redisCon = ConnectionMultiplexer.Connect("localhost");
            var cache = redisCon.GetDatabase();
            var devicesCount = 10000;
            for (int i = 0; i < devicesCount; i++)
            {
                var value = await cache.StringGetAsync($"person{i}");
                Console.WriteLine($"Valor={value}");
            }

            return new List<History> { new History("john chicken", new DateTime(2021, 03, 03, 12, 07, 06), "my property", "DECLINED") };
        }

        //private async Task<IEnumerable<History>> SaveToCacheAsync(string input)
        //{
        //    using var redisCon = ConnectionMultiplexer.Connect("localhost");
        //    var cache = redisCon.GetDatabase();
        //    var devicesCount = 10000;
        //    for (int i = 0; i < devicesCount; i++)
        //    {
        //        var value = await cache.StringSetAsync($"person{i}", $"{i}", TimeSpan.FromMinutes(5));
        //        Console.WriteLine($"Valor={value}");
        //    }

        //    return new List<History> { new History("john chicken", new DateTime(2021, 03, 03, 12, 07, 06), "my property", "DECLINED") };
        //}
    }

    public class History
    {
        public History(string visitorName, DateTime visitDate, string property, string status)
        {
            VisitorName = visitorName;
            VisitDate = visitDate;
            Property = property;
            Status = status;
        }

        public string VisitorName { get; set; }
        public DateTime VisitDate { get; set; }
        public string Property { get; set; }
        public string Status { get; set; }

        //public override bool Equals(object obj)
        //{
        //    var objectgiven = obj as History;

        //    if (objectgiven is null)
        //    {
        //        return false;
        //    }

        //    if (objectgiven.VisitorName == VisitorName &&
        //        objectgiven.VisitDate.ToString() == VisitDate.ToString() &&
        //        objectgiven.Property == Property &&
        //        objectgiven.Status == Status)
        //        return true;

        //    return false;
        //}
        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();  
        }

    }

    class Name
    {
        public Name(string firstName, string lastName)
        {
            FirstName = firstName;
            LastName = lastName;
        }

        public string FirstName { get; set; }
        public string LastName { get; set; }
    }

}
