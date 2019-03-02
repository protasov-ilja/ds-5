using StackExchange.Redis;

namespace Producer
{
    internal class Program
    {
        const string JOB_HINTS_CHANNEL = "job_hints";
        const string JOBS_QUEUE_NAME = "jobs_queue";

        private static void Main( string[] args )
        {
            string redisUrl = args.Length > 0 ? args[0] : "localhost";
            IConnectionMultiplexer redis = ConnectionMultiplexer.Connect( redisUrl );

            var msg1 = BuildJobMessage( "this is the first job" );
            SendMessage( msg1, redis );

            var msg2 = BuildJobMessage( "this is the second one" );
            SendMessage( msg2, redis );
        }

        private static void SendMessage(string message, IConnectionMultiplexer redis )
        {
            // put message to queue
            redis.GetDatabase().ListLeftPush( JOBS_QUEUE_NAME, message, flags: CommandFlags.FireAndForget );
            // and notify consumers
            redis.GetSubscriber().Publish( JOB_HINTS_CHANNEL, "" );
        }

        private static string BuildJobMessage(string data)
        {
            return $"JOB:{data}";
        }
    }
}
