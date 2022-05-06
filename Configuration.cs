using System;


namespace TelemetryCore
{
    public class Configuration
    {
        public double sampleRate { get; set; } = 1;


        public bool enableKafkaStreaming { get; set; } = false;
        public bool enableMongoStreaming { get; set; } = false;
        public bool enableInfluxStreaming { get; set; } = false;

        public int gameDataPort { get; set; } = 5300;


        public string mongouri { get; set; } = "mongodb://localhost:27017";
        public string database { get; set; } = "telemetry";
        public string collection { get; set; } = "gamedata";

        public string influxHost { get; set; } = "localhost:8088";
        public string bucket { get; set; } = "gameTelemetry";
        public string token { get; set; } = "token";
        public string org { get; set; } = "org";

        public string kafkaServer { get; set; } = "localhost:9092";
        public string topic { get; set; } = "gameTelemetry";

        public bool verbose { get; set; } = true;
    }
 }
