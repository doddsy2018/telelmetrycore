using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

using Confluent.Kafka;
using MongoDB.Bson;
using MongoDB.Driver;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Core;
using InfluxDB.Client.Writes;
using System.Diagnostics;
using System.Threading;

namespace TelemetryCore
{
    class Program
    {
        private static Configuration appConfig;

        private static bool enableKafkaStreaming = false;
        private static bool enableMongoStreaming = false;
        private static bool enableInfluxStreaming = false;

        private static double sampleRate = 10;
        private static int recordRateMS = 100;
        private static int FORZA_DATA_OUT_PORT = 5300;

        private static int recordTolerance = 5;

        private static bool recordingData = false;
        private static bool streamToKafka = false;
        private static bool streamToMongo = false;
        private static bool streamToConsole = false;
        private static bool streamToInflux = false;

        private static bool isRaceOn = false;
        private static DataPacket data = new DataPacket();
        private static string currentFilename = "./data/" + DateTime.Now.ToFileTime() + ".csv";

        private static ProducerWrapper myProducer;
        private static IMongoCollection<BsonDocument> collection;

        private static string bucket = "data";
        private static string org = "org";
        private static InfluxDBClient influxClient;

        private static bool verboseOutput = false;

        private static StreamWriter sw;

        static void Main(string[] args)
        {

            showSplash();

            DisplayTimerProperties();

            // Read the config file
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .Build();
            appConfig = deserializer.Deserialize<Configuration>(System.IO.File.ReadAllText("config.yaml"));

            sampleRate = appConfig.sampleRate;
            FORZA_DATA_OUT_PORT = appConfig.gameDataPort;
            enableKafkaStreaming = appConfig.enableKafkaStreaming;
            enableMongoStreaming = appConfig.enableMongoStreaming;
            enableInfluxStreaming = appConfig.enableInfluxStreaming;
            verboseOutput = appConfig.verbose;

            double dt = 1 / sampleRate;
            recordRateMS = (int)Math.Round(dt * 1000);
            Console.WriteLine($"sampleRate: {sampleRate}, recordRateMS: {recordRateMS}");

            string directory = "./data";
            Directory.CreateDirectory(directory);

            #region connection setups
            if (enableKafkaStreaming) {
                Console.WriteLine("Starting Kafka Producer");
                var config = new ProducerConfig
                {
                    BootstrapServers = appConfig.kafkaServer,
                    ClientId = Dns.GetHostName(),
                };
                myProducer = new ProducerWrapper(config, appConfig.topic);
            }


            if (enableMongoStreaming)
            {
                Console.WriteLine("Connecting to mongoDB ");
                string collectionName = appConfig.collection;
                var client = new MongoClient(appConfig.mongouri);
                var database = client.GetDatabase(appConfig.database);
                var collectList = database.ListCollectionNames().ToList();
                if (collectList.Contains(collectionName) == false)
                {
                    Console.WriteLine("Timeseries Collection Does not exist - creating");
                    database.CreateCollection(collectionName, new CreateCollectionOptions { TimeSeriesOptions = new TimeSeriesOptions("timestamp", "metadata") });
                }
                else
                {
                    Console.WriteLine("Found Existing Timeseries Collection - using it");
                }

                collection = database.GetCollection<BsonDocument>(collectionName);
            }


            if (enableInfluxStreaming)
            {
                Console.WriteLine("Connecting to InfluxDB ");
                bucket = appConfig.bucket;
                org = appConfig.org;

                // To Do - Autocreate the bucket if it does not exist
            }
            #endregion

            #region process udp message
            var ipEndPoint = new IPEndPoint(IPAddress.Loopback, FORZA_DATA_OUT_PORT);
            var receiverTask = Task.Run(async () =>
            {
                var client = new UdpClient(FORZA_DATA_OUT_PORT);
                while (true)
                {
                    await client.ReceiveAsync().ContinueWith(receive =>
                    {
                        var resultBuffer = receive.Result.Buffer;
                        if (!AdjustToBufferType(resultBuffer.Length))
                        {
                            Console.WriteLine($"buffer not the correct length. length is {resultBuffer.Length}");
                            return;
                        }
                        isRaceOn = resultBuffer.IsRaceOn();

                        // parse data
                        if (resultBuffer.IsRaceOn())
                        {
                            data = ParseData(resultBuffer);
                            SendData(data);
                        }
                        
                    });
                }
            });
            #endregion

            #region csv recorder
            var recorderTask = Task.Run(async () =>
            {
                var stageStopWatch = new Stopwatch();
                var processStopWatch = new Stopwatch();
                while (true)
                {
                    if (isRaceOn && recordingData)
                    {
                        stageStopWatch.Start();
                        processStopWatch.Start();

                        await RecordData(data);
                        //await Task.Delay(recordRateMS);

                        stageStopWatch.Stop();
                        var recordTime = stageStopWatch.Elapsed.TotalMilliseconds;
                        stageStopWatch.Reset();
                        stageStopWatch.Start();

                        recordDelay(recordRateMS - Convert.ToInt32(recordTime));

                        stageStopWatch.Stop();
                        processStopWatch.Stop();
                        var delayTime = stageStopWatch.Elapsed.TotalMilliseconds;
                        var processTime = processStopWatch.Elapsed.TotalMilliseconds;
                        if (processTime > recordRateMS + recordTolerance)
                        {
                            Console.WriteLine($"csv processTime={processTime}, recordTime ={recordTime}, delayTime={delayTime}");
                        }
                        stageStopWatch.Reset();
                        processStopWatch.Reset();
                        if (verboseOutput)
                        {
                            Console.WriteLine($"csv processTime={processTime}");
                        }
                    }
                }
            });
            #endregion

            #region Kafka Streamer
            var sendToKafka = Task.Run(async () =>
            {
                var stageStopWatch = new Stopwatch();
                var processStopWatch = new Stopwatch(); ;

                while (true)
                {
                    if (isRaceOn && streamToKafka)
                    {
                        stageStopWatch.Start();
                        processStopWatch.Start();

                        // Has the stream updated and reporting correctly
                        if (data.CarOrdinal != 0)
                        {
                            // Set the msgTimestamp
                            long millisecondsTS = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                            //long microsecondsTS = (long)((DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds * 1000.0);
                            //long nanosecondsTS = (long)((DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds * 1000000.0);
                            data.msgTimestamp = millisecondsTS;
                            string dataString = JsonSerializer.Serialize(data);
                            await myProducer.writeMessage($"{dataString}");
                        }
                        stageStopWatch.Stop();
                        var recordTime = stageStopWatch.Elapsed.TotalMilliseconds;
                        stageStopWatch.Reset();
                        stageStopWatch.Start();

                        //await Task.Delay(recordRateMS);
                        recordDelay(recordRateMS-Convert.ToInt32(recordTime));

                        stageStopWatch.Stop();
                        processStopWatch.Stop();
                        var delayTime = stageStopWatch.Elapsed.TotalMilliseconds;
                        var processTime = processStopWatch.Elapsed.TotalMilliseconds;
                        if (processTime > recordRateMS + recordTolerance)
                        {
                            Console.WriteLine($"Kafka processTime={processTime}, recordTime ={recordTime}, delayTime={delayTime}");
                        }
                        stageStopWatch.Reset();
                        processStopWatch.Reset();
                        if (verboseOutput)
                        {
                            Console.WriteLine($"Kafka processTime={processTime}");
                        }
                    }
                }
            });
            #endregion

            #region Mongo streamer
            var sendToMongo = Task.Run(async () =>
            {
                var stageStopWatch = new Stopwatch();
                var processStopWatch = new Stopwatch();

                while (true)
                {
                    if (isRaceOn && streamToMongo)
                    {
                        stageStopWatch.Start();
                        processStopWatch.Start();

                        // Has the stream updated and reporting correctly
                        if (data.CarOrdinal != 0)
                        {
                            // Set the msgTimestamp
                            long millisecondsTS = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                            data.msgTimestamp = millisecondsTS;

                            var metadata = new BsonDocument
                            {
                                { "CarOrdinal",  data.CarOrdinal },
                                { "CarClass",  data.CarClass  },
                            };

#pragma warning disable CS1062 // The best overloaded Add method for the collection initializer element is obsolete
                            var document = new BsonDocument
                            {
                                { "timestamp",  DateTime.Now },
                                { "metadata",  metadata  },
                                { BsonDocument.Parse(JsonSerializer.Serialize(data)) },
                            };
#pragma warning restore CS1062 // The best overloaded Add method for the collection initializer element is obsolete

                            //Console.WriteLine($"{document}");

                            await collection.InsertOneAsync(document);
                        }

                        stageStopWatch.Stop();
                        var recordTime = stageStopWatch.Elapsed.TotalMilliseconds;
                        stageStopWatch.Reset();
                        stageStopWatch.Start();

                        //await Task.Delay(recordRateMS);
                        recordDelay(recordRateMS - Convert.ToInt32(recordTime));

                        stageStopWatch.Stop();
                        processStopWatch.Stop();
                        var delayTime = stageStopWatch.Elapsed.TotalMilliseconds;
                        var processTime = processStopWatch.Elapsed.TotalMilliseconds;
                        if (processTime > recordRateMS + recordTolerance)
                        {
                            Console.WriteLine($"Mongo processTime={processTime}, recordTime ={recordTime}, delayTime={delayTime}");
                        }
                        stageStopWatch.Reset();
                        processStopWatch.Reset();
                        if (verboseOutput) {
                            Console.WriteLine($"Mongo processTime={processTime}");
                        }
                    }
                }
            });
            #endregion

            #region Influx streamer
            //TODO Look into faster relflection and batch send to influx
            var sendToInflux = Task.Run(async () =>
            {
                var stageStopWatch = new Stopwatch();
                var processStopWatch = new Stopwatch();
                double serializeTime = 0;
                influxClient = InfluxDBClientFactory.Create(appConfig.influxHost, appConfig.token);
                var writeApiAsync = influxClient.GetWriteApi();

                while (true)
                {
                    if (isRaceOn && streamToInflux)
                    {
                        stageStopWatch.Start();
                        processStopWatch.Start();

                        // Has the stream updated and reporting correctly
                        if (data.CarOrdinal != 0)
                        {
                            // Set the msgTimestamp
                            long millisecondsTS = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                            data.msgTimestamp = millisecondsTS;

                            long nanosecondsTS = (long)((DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds * 1000000.0);

                            string mem = data.CarOrdinal.ToString();
                            string sensorString = "";

                            foreach (var prop in data.GetType().GetProperties())
                            {
                                //if (prop.Name != "IsRaceOn"){
                                //    string sensorReading = $"{prop.Name}={prop.GetValue(data, null)}";
                                //    sensorString = sensorString + sensorReading + ",";  
                                //}
                                string sensorReading = $"{prop.Name}={prop.GetValue(data, null)}";
                                sensorString = sensorString + sensorReading + ",";
                            }

                            sensorString = sensorString.Remove(sensorString.Length - 1);
                            //Console.WriteLine(sensorString);

                            string dataObjString = $"{mem} {sensorString} {nanosecondsTS}";


                            stageStopWatch.Stop();
                            serializeTime = stageStopWatch.Elapsed.TotalMilliseconds;
                            stageStopWatch.Reset();
                            stageStopWatch.Start();

                            
                            writeApiAsync.WriteRecord(dataObjString, WritePrecision.Ns, bucket, org);

                            //using (var writeApi = influxClient.GetWriteApi())
                            //{
                            //    writeApi.WriteRecord(dataObjString, WritePrecision.Ns, bucket, org);
                            //}

                            //await writeStringToInflux(dataObjString);

                        }

                        stageStopWatch.Stop();
                        var recordTime = stageStopWatch.Elapsed.TotalMilliseconds;
                        stageStopWatch.Reset();
                        stageStopWatch.Start();

                        //await Task.Delay(recordRateMS);
                        //await Task.Delay(0);  // dummy await
                        recordDelay(recordRateMS - Convert.ToInt32(recordTime));

                        stageStopWatch.Stop();
                        processStopWatch.Stop();
                        var delayTime = stageStopWatch.Elapsed.TotalMilliseconds;
                        var processTime = processStopWatch.Elapsed.TotalMilliseconds;
                        if (processTime > recordRateMS + recordTolerance)
                        {
                            Console.WriteLine($"Influx processTime={processTime}, serializeTime={serializeTime}, recordTime ={recordTime}, delayTime={delayTime}");
                        }
                        stageStopWatch.Reset();
                        processStopWatch.Reset();
                        if (verboseOutput)
                        {
                            Console.WriteLine($"Influx processTime={processTime}");
                        }
                    }
                }
            });
            #endregion

            #region AppControls
            Console.WriteLine("Press S to stop");
            Console.WriteLine("Press Q to display current recorder states");
            Console.WriteLine("Press C to toggle Console Output");
            Console.WriteLine("Press M to toggle Mongo Output");
            Console.WriteLine("Press I to toggle Influx Output");
            Console.WriteLine("Press K to toggle Kafka Output");

            while (!Console.KeyAvailable)
            {
                string command=Console.ReadKey(true).Key.ToString();
                if (command == "R")
                {
                    if (recordingData)
                    {
                        StopRecordingSession();
                    }
                    else
                    {
                        StartNewRecordingSession();
                    }
                }


                if (command == "K")
                {
                    if (enableKafkaStreaming) {
                        if (streamToKafka)
                        {
                            streamToKafka = false;
                            Console.WriteLine("Stream to Kafka stopped");
                        }
                        else
                        {
                            streamToKafka = true;
                            Console.WriteLine("Stream to Kafka started");
                        }
                    }
                    else
                    {
                        Console.WriteLine("Streaming to kafka is disabled - edit config");
                    }
                }


                if (command == "M")
                {
                    if (enableMongoStreaming)
                    {
                        if (streamToMongo)
                        {
                            streamToMongo = false;
                            Console.WriteLine("Stream to Mongo stopped");
                        }
                        else
                        {
                            streamToMongo = true;
                            Console.WriteLine("Stream to mongo started");
                        }
                    }
                    else
                    {
                        Console.WriteLine("Streaming to mongo is disabled - edit config");
                    }
                }

                if (command == "I")
                {
                    if (enableInfluxStreaming)
                    {
                        if (streamToInflux)
                        {
                            streamToInflux = false;
                            Console.WriteLine("Stream to Influx stopped");
                        }
                        else
                        {
                            streamToInflux = true;
                            Console.WriteLine("Stream to Influx started");
                        }
                    }
                    else
                    {
                        Console.WriteLine("Streaming to influx is disabled - edit config");
                    }
                }





                if (command == "C")
                {
                    if (streamToConsole)
                    {
                        streamToConsole = false;
                        Console.WriteLine("Stream to Console stopped");
                    }
                    else
                    {
                        streamToConsole = true;
                        Console.WriteLine("Stream to Console started");
                    }
                }

                if (command == "S")
                {
                    break;
                }


                if (command == "Q")
                {
                    Console.WriteLine("\n*** Recorder States ***");
                    Console.WriteLine($"enableKafkaStreaming: {enableKafkaStreaming}\n" +
                        $"enableMongoStreaming: {enableMongoStreaming}\n" +
                        $"enableInfluxStreaming: {enableInfluxStreaming}\n");

                    Console.WriteLine($"streamToConsole: {streamToConsole}\n" +
                        $"recordingData: {recordingData}\n" +
                        $"streamToKafka: {streamToKafka}\n" +
                        $"streamToMongo: {streamToMongo}\n" +
                        $"streamToMongo: {streamToInflux}\n");
                }
            }

            #endregion

        }

        static void SendData(DataPacket data)
        {
            if (streamToConsole) {
                //string dataString = JsonSerializer.Serialize(data);
                Console.WriteLine($"{data.Speed}, {data.CurrentEngineRpm}");
            }
        }

        static async Task RecordData(DataPacket data)
        {
            // Has the stream updated and reporting correctly
            if (data.CarOrdinal != 0) {
                // Set the msgTimestamp
                long millisecondsTS = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                data.msgTimestamp = millisecondsTS;

                string dataToWrite = DataPacketToCsvString(data);
                //const int BufferSize = 131072;  // 128K  65536;  // 64 Kilobytes
                //StreamWriter sw = new StreamWriter(currentFilename, true, Encoding.UTF8, BufferSize);
                await sw.WriteLineAsync(dataToWrite);
                //sw.Close();

            }
        }

        static DataPacket ParseData(byte[] packet)
        {
            DataPacket data = new DataPacket();

            // sled
            data.IsRaceOn = packet.IsRaceOn();
            data.gameTimestampMS = packet.TimestampMs(); 
            data.EngineMaxRpm = packet.EngineMaxRpm(); 
            data.EngineIdleRpm = packet.EngineIdleRpm(); 
            data.CurrentEngineRpm = packet.CurrentEngineRpm(); 
            data.AccelerationX = packet.AccelerationX(); 
            data.AccelerationY = packet.AccelerationY(); 
            data.AccelerationZ = packet.AccelerationZ(); 
            data.VelocityX = packet.VelocityX(); 
            data.VelocityY = packet.VelocityY(); 
            data.VelocityZ = packet.VelocityZ(); 
            data.AngularVelocityX = packet.AngularVelocityX(); 
            data.AngularVelocityY = packet.AngularVelocityY(); 
            data.AngularVelocityZ = packet.AngularVelocityZ(); 
            data.Yaw = packet.Yaw(); 
            data.Pitch = packet.Pitch(); 
            data.Roll = packet.Roll(); 
            data.NormalizedSuspensionTravelFrontLeft = packet.NormSuspensionTravelFl(); 
            data.NormalizedSuspensionTravelFrontRight = packet.NormSuspensionTravelFr(); 
            data.NormalizedSuspensionTravelRearLeft = packet.NormSuspensionTravelRl(); 
            data.NormalizedSuspensionTravelRearRight = packet.NormSuspensionTravelRr(); 
            data.TireSlipRatioFrontLeft = packet.TireSlipRatioFl(); 
            data.TireSlipRatioFrontRight = packet.TireSlipRatioFr(); 
            data.TireSlipRatioRearLeft = packet.TireSlipRatioRl(); 
            data.TireSlipRatioRearRight = packet.TireSlipRatioRr(); 
            data.WheelRotationSpeedFrontLeft = packet.WheelRotationSpeedFl(); 
            data.WheelRotationSpeedFrontRight = packet.WheelRotationSpeedFr(); 
            data.WheelRotationSpeedRearLeft = packet.WheelRotationSpeedRl(); 
            data.WheelRotationSpeedRearRight = packet.WheelRotationSpeedRr(); 
            data.WheelOnRumbleStripFrontLeft = packet.WheelOnRumbleStripFl(); 
            data.WheelOnRumbleStripFrontRight = packet.WheelOnRumbleStripFr(); 
            data.WheelOnRumbleStripRearLeft = packet.WheelOnRumbleStripRl(); 
            data.WheelOnRumbleStripRearRight = packet.WheelOnRumbleStripRr(); 
            data.WheelInPuddleDepthFrontLeft = packet.WheelInPuddleFl(); 
            data.WheelInPuddleDepthFrontRight = packet.WheelInPuddleFr(); 
            data.WheelInPuddleDepthRearLeft = packet.WheelInPuddleRl(); 
            data.WheelInPuddleDepthRearRight = packet.WheelInPuddleRr(); 
            data.SurfaceRumbleFrontLeft = packet.SurfaceRumbleFl(); 
            data.SurfaceRumbleFrontRight = packet.SurfaceRumbleFr(); 
            data.SurfaceRumbleRearLeft = packet.SurfaceRumbleRl(); 
            data.SurfaceRumbleRearRight = packet.SurfaceRumbleRr(); 
            data.TireSlipAngleFrontLeft = packet.TireSlipAngleFl(); 
            data.TireSlipAngleFrontRight = packet.TireSlipAngleFr(); 
            data.TireSlipAngleRearLeft = packet.TireSlipAngleRl(); 
            data.TireSlipAngleRearRight = packet.TireSlipAngleRr(); 
            data.TireCombinedSlipFrontLeft = packet.TireCombinedSlipFl(); 
            data.TireCombinedSlipFrontRight = packet.TireCombinedSlipFr(); 
            data.TireCombinedSlipRearLeft = packet.TireCombinedSlipRl(); 
            data.TireCombinedSlipRearRight = packet.TireCombinedSlipRr(); 
            data.SuspensionTravelMetersFrontLeft = packet.SuspensionTravelMetersFl(); 
            data.SuspensionTravelMetersFrontRight = packet.SuspensionTravelMetersFr(); 
            data.SuspensionTravelMetersRearLeft = packet.SuspensionTravelMetersRl(); 
            data.SuspensionTravelMetersRearRight = packet.SuspensionTravelMetersRr();
            data.CarOrdinal = packet.CarOrdinal(); 
            data.CarClass = packet.CarClass();
            data.CarPerformanceIndex = packet.CarPerformanceIndex();
            data.DrivetrainType = packet.DriveTrain();
            data.NumCylinders = packet.NumCylinders();

            // dash
            data.PositionX = packet.PositionX();
            data.PositionY = packet.PositionY();
            data.PositionZ = packet.PositionZ();
            data.Speed = packet.Speed();
            data.Power = packet.Power();
            data.Torque = packet.Torque();
            data.TireTempFl = packet.TireTempFl();
            data.TireTempFr = packet.TireTempFr();
            data.TireTempRl = packet.TireTempRl();
            data.TireTempRr = packet.TireTempRr();
            data.Boost = packet.Boost();
            data.Fuel = packet.Fuel();
            data.Distance = packet.Distance();
            data.BestLapTime = packet.BestLapTime();
            data.LastLapTime = packet.LastLapTime();
            data.CurrentLapTime = packet.CurrentLapTime();
            data.CurrentRaceTime = packet.CurrentRaceTime();
            data.Lap = packet.Lap();
            data.RacePosition = packet.RacePosition();
            data.Accelerator = packet.Accelerator();
            data.Brake = packet.Brake();
            data.Clutch = packet.Clutch();
            data.Handbrake = packet.Handbrake();
            data.Gear = packet.Gear();
            data.Steer = packet.Steer();
            data.NormalDrivingLine = packet.NormalDrivingLine();
            data.NormalAiBrakeDifference = packet.NormalAiBrakeDifference();
            
            return data;
        }

        static bool AdjustToBufferType(int bufferLength)
        {
            switch (bufferLength)
            {
                case 232: // FM7 sled
                    return false;
                case 311: // FM7 dash
                    FMData.BufferOffset = 0;
                    return true;
                case 324: // FH4
                    FMData.BufferOffset = 12;
                    return true;
                default:
                    return false;
            }
        }

        static void StartNewRecordingSession()
        {
            currentFilename = "./data/" + DateTime.Now.ToFileTime() + ".csv";
            recordingData = true;

            IEnumerable<string> props = data.GetType()
                 .GetProperties()
                 .Where(p => p.CanRead)
                 .Select(p => p.Name);
            StringBuilder sb = new StringBuilder();
            sb.AppendJoin(',', props);

            //StreamWriter sw = new StreamWriter(currentFilename, true, Encoding.UTF8);

            //const int BufferSize = 65536;  // 64 Kilobytes
            const int BufferSize = 131072;  // 128K  65536
            sw = new StreamWriter(currentFilename, true, Encoding.UTF8, BufferSize);

            sw.WriteLine(sb.ToString());
            //sw.Close();
            Console.WriteLine($"CSV Recording started - {currentFilename} ");
        }

        static void StopRecordingSession()
        {
            recordingData = false;
            sw.Close();
            Console.WriteLine("CSV Recording stopped");
        }

        static string DataPacketToCsvString(DataPacket packet)
        {
            IEnumerable<object> values = data.GetType()
                 .GetProperties()
                 .Where(p => p.CanRead)
                 .Select(p => p.GetValue(packet, null));

            StringBuilder sb = new StringBuilder();
            sb.AppendJoin(',', values);
            return sb.ToString();
        }


        static void recordDelay(int delayMS)
        {
            Stopwatch oStopWatch = new Stopwatch();
            oStopWatch.Start();
            while (oStopWatch.ElapsedMilliseconds < delayMS)
            {
                // Do Nothing
            }
        }

        // To Be Removed
        static async Task<Task> writeStringToInflux(string theString)
        {
            using (var writeApi = influxClient.GetWriteApi())
            {
                writeApi.WriteRecord(theString, WritePrecision.Ns, bucket, org);
            }
            await Task.Delay(0);  // dummy await
            return Task.CompletedTask;
        }

        public static void DisplayTimerProperties()
        {
            // Display the timer frequency and resolution.
            if (Stopwatch.IsHighResolution)
            {
                Console.WriteLine("Operations timed using the system's high-resolution performance counter.");
            }
            else
            {
                Console.WriteLine("Operations timed using the DateTime class.");
            }

            long frequency = Stopwatch.Frequency;
            Console.WriteLine("  Timer frequency in ticks per second = {0}",
                frequency);
            long nanosecPerTick = (1000L * 1000L * 1000L) / frequency;
            Console.WriteLine("  Timer is accurate within {0} nanoseconds",
                nanosecPerTick);
        }

        static void showSplash()
        {
            Console.WriteLine(
                "\n" +
                "░▀█▀░█▀▀░█░░░█▀▀░█▄█░█▀▀░▀█▀░█▀▄░█░█░░░█▀▀░█▀█░█▀▄░█▀▀\n" +
                "░░█░░█▀▀░█░░░█▀▀░█░█░█▀▀░░█░░█▀▄░░█░░░░█░░░█░█░█▀▄░█▀▀\n" +
                "░░▀░░▀▀▀░▀▀▀░▀▀▀░▀░▀░▀▀▀░░▀░░▀░▀░░▀░░░░▀▀▀░▀▀▀░▀░▀░▀▀▀\n" +
                "\n" +
                "(C) 2022 Iain J Dodds" + 
                "\n"
                );
        }
    }
}
