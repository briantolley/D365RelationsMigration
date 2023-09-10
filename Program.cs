using Relationship2ConnectionMigration.Services;

namespace Relationship2ConnectionMigration
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Engine engine = new Engine();
            engine.Run().Wait();
        }
    }
}