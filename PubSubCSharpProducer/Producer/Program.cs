using System;
using System.IO;
using Google.Apis.Logging;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Grpc.Core.Logging;
using MsgPack.Serialization;
using Newtonsoft.Json;

namespace Producer
{
    public class GameEvent
    {
        [JsonProperty(PropertyName = "gameId")]
        public Guid GameId { get; set; }

        [JsonProperty(PropertyName = "creationDate")]
        public DateTime CreationDate { get; set; }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            string projectId = "my-project-id";

            //Set Auth Json File
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", "./my-json-auth-file.json");

            //Set Logger
            GrpcEnvironment.SetLogger(new Grpc.Core.Logging.ConsoleLogger());

            //Define Publisher
            PublisherClient publisher = PublisherClient.Create();

            //Define Topic
            var topicName = new TopicName(projectId, "GameEvents");

            //Create Topic            
            try
            {
                Topic topic = publisher.CreateTopic(topicName);
            }
            catch (Exception ex)
            {
                //I.e. Topic Already Exists
                Console.WriteLine(ex);
            }

            //Publish Messages
            using (var rateLimiter = new RateLimiter(10, TimeSpan.FromSeconds(1)))
            {
                while (true)
                {
                    rateLimiter.WaitToProceed();

                    //Generate Random Event
                    var gameEvent = new GameEvent() { GameId = Guid.NewGuid(), CreationDate = DateTime.UtcNow };

                    //MsgPck-Cli
                    //var serializer = SerializationContext.Default.GetSerializer<GameEvent>();
                    //var stream = new MemoryStream();
                    //serializer.Pack(stream, gameEvent);
                    //var data = Google.Protobuf.ByteString.FromStream(stream);

                    //Json
                    var gameEventJsonStr = JsonConvert.SerializeObject(gameEvent);
                    var data = Google.Protobuf.ByteString.CopyFromUtf8(gameEventJsonStr);

                    PubsubMessage message = new PubsubMessage
                    {
                        Data = data,
                        Attributes = { { "MetaData-EnvName", "DevEnv" }, { "Timestamp", DateTime.UtcNow.ToString("O") } }
                    };
                    publisher.Publish(topicName, new[] { message });
                }
            }
        }
    }
}
