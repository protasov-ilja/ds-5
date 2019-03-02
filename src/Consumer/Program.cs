using StackExchange.Redis;
using System;

namespace Consumer
{
    internal class Program
    {
        const string QUEUE_NAME = "JOBS_QUEUE";
        const string HINTS_CHANNEL = "JOBS_QUEUE_hints";

        public static void Main( string[] args )
        {
            string redisUrl = args.Length > 0 ? args[0] : "localhost";
            IConnectionMultiplexer redis = ConnectionMultiplexer.Connect( redisUrl );

            // subscribe to notifications
            redis.GetSubscriber().Subscribe( HINTS_CHANNEL, delegate
             {
                 // process all messages in queue
                 string msg = redis.GetDatabase().ListRightPop( QUEUE_NAME );
                 while (msg != null)
                 {
                     string jobData = ParseData( msg );
                     DoJob( jobData );
                     msg = redis.GetDatabase().ListRightPop( QUEUE_NAME );
                 }
             } );

            redis.GetSubscriber().Publish( HINTS_CHANNEL, "" );

            Console.WriteLine( "Press Enter to exit" );
            Console.ReadLine();
        }

        private static void DoJob( string jobData )
        {
            Console.WriteLine( $"Job data: {jobData}" );
            System.Threading.Thread.Sleep(1500); // emulate loading
        }

        private static string ParseData( string msg )
        {
            return msg.Split( ':' )[1];
        }


    }
}
