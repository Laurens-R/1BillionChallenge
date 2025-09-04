// See https://aka.ms/new-console-template for more information

using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Text;
using runner;

namespace runner;

static class Program
{
    static async Task Main(string[] args)
    {
        await ParseCsv();
    }

    private static async Task ParseCsv()
    {
        var dataPath = @"D:\1brow\small.txt";

        Console.WriteLine($"Processing data from {dataPath}...");

        Stopwatch timer = new();
        timer.Start();

        var fileInfo = new FileInfo(dataPath);

        if (fileInfo.Exists)
        {
            // Create or open the file
            using var fileStream = new FileStream(dataPath, FileMode.Open, FileAccess.Read, FileShare.Read);

            // Create memory-mapped file
            using var mmf = MemoryMappedFile.CreateFromFile(
                fileStream,
                null,
                fileInfo.Length,
                MemoryMappedFileAccess.Read,
                HandleInheritability.None,
                leaveOpen: true);

            Console.WriteLine("Preparing chunks...");

            List<Chunk> chunks = new();
            PrepareChunks(fileInfo, mmf, chunks);
            var results = await ProcessData(chunks, mmf);

            foreach (var key in results.Keys)
            {
                var station = results[key];
                Console.WriteLine($"{station.Name}: high: {station.Highest}, low: {station.Lowest}");
            }
        }

        else
        {
            Console.WriteLine("File not found");
        }


        timer.Stop();

        Console.WriteLine($"Total time taken: {timer.Elapsed.TotalSeconds}");
    }

    static void PrepareChunks(FileInfo fileInfo, MemoryMappedFile mmf, List<Chunk> chunks)
    {
        long fileSize = fileInfo.Length;
        long chunkSize = fileInfo.Length / (Environment.ProcessorCount); // 1MB
        long offset = 0;

        while (offset < fileSize)
        {
            long tentativeEnd = Math.Min(offset + chunkSize, fileSize);
            long scanOffset = tentativeEnd - 1;

            using var accessor = mmf.CreateViewAccessor(offset, tentativeEnd - offset, MemoryMappedFileAccess.Read);

            long relativeOffset = scanOffset - offset;
            byte b = accessor.ReadByte(relativeOffset);

            while (b != 0x0A && relativeOffset > 0)
            {
                relativeOffset--;
                b = accessor.ReadByte(relativeOffset);
            }

            long actualEnd = offset + relativeOffset;

            chunks.Add(new Chunk
            {
                From = offset,
                To = actualEnd
            });

            offset = actualEnd + 1;
        }
    }
    
    static async Task<Dictionary<int, WeatherStation>> ProcessData(List<Chunk> list, MemoryMappedFile memoryMappedFile)
    {
        Console.WriteLine("Processing chunks...");
        
        int bufferSize = 1024 * 8;
        List<Dictionary<int, WeatherStation>> results = new();
        
        await Parallel.ForAsync(0, list.Count, async (i, ct) =>
        {
            Dictionary<int, WeatherStation> stations = new();
            var chunk = list[i];
            int totalBufferSize = bufferSize * 2;
            byte[] buffer = new byte[totalBufferSize];
            char[] tokenBuffer = new char[100];
            int tokenBufferOffset = 0;
            int bufferOffset = 0;
            
            using var reader =
                memoryMappedFile.CreateViewStream(chunk.From, chunk.Size, MemoryMappedFileAccess.Read);

            var bytesRead = reader.ReadAsync(buffer, bufferOffset, bufferSize);

            while (await bytesRead != 0)
            {
                int bytesReadCount = bytesRead.Result;
                int processingOffset = bufferOffset;

                //first read the next batch of bytes asynchronously
                bufferOffset = (bufferOffset + bufferSize) % totalBufferSize;
                bytesRead = reader.ReadAsync(buffer, bufferOffset, bufferSize);

                //Console.WriteLine($"Processing chunk {i} at offset {processingOffset} with {bytesReadCount} bytes");
                ReadOnlySpan<char> bufferString = Encoding.Default.GetString(buffer, processingOffset, bytesReadCount);

                double temperature = 0.0;
                string station = String.Empty;

                //and while the IO system is doing that... we proceed with processing the current batch
                for (int charOffset = 0; charOffset < bufferString.Length; charOffset++)
                {
                    char c = bufferString[charOffset];
                    switch (c)
                    {
                        case '\n':
                        {
                            var valueToken = tokenBuffer.AsSpan(0, tokenBufferOffset);
                            tokenBufferOffset = 0;

                            temperature = ParseDouble(valueToken, 0, valueToken.Length);

                            if (stations.TryGetValue(station.GetHashCode(), out var weatherStation))
                            {
                                weatherStation.Highest = Math.Max(weatherStation.Highest, temperature);
                                weatherStation.Lowest = Math.Min(weatherStation.Lowest, temperature);
                            }
                            else
                            {
                                var newStation = new WeatherStation
                                {
                                    Name = station,
                                    Highest = temperature,
                                    Lowest = temperature
                                };
                                stations.Add(station.GetHashCode(), newStation);
                            }
                        } break;
                        case ';':
                        {
                            station = tokenBuffer.AsSpan(0, tokenBufferOffset).ToString();
                            tokenBufferOffset = 0;    
                        } break;
                        default:
                        {
                            tokenBuffer[tokenBufferOffset] = c;
                            tokenBufferOffset++;    
                        } break;
                    }
                }
            }

            lock (results)
            {
                results.Add(stations);
            }
        });

        Console.WriteLine("Merging results...");
        
        Dictionary<int, WeatherStation> mergedResults = new();
        foreach (var result in results)
        {
            foreach (var station in result.Values)
            {
                if(mergedResults.TryGetValue(station.Name.GetHashCode(), out var mergedStation))
                {
                    mergedStation.Highest = Math.Max(mergedStation.Highest, station.Highest);
                    mergedStation.Lowest = Math.Min(mergedStation.Lowest, station.Lowest);
                    mergedResults[station.Name.GetHashCode()] = mergedStation;   
                }
                else
                {
                    mergedResults.Add(station.Name.GetHashCode(), station);   
                }
            }
        }
        
        return mergedResults;
    }

    /// <summary>
    /// Specialize ParseDouble method specifically for the format of the data we are parsing.
    /// </summary>
    /// <param name="buffer"></param>
    /// <param name="offset"></param>
    /// <param name="length"></param>
    /// <returns></returns>
    public static double ParseDouble(Span<char> buffer, int offset, int length)
    {
        bool negative = buffer[offset] == 45;
        offset = (negative) ? offset + 1 : offset;

        //X.X
        if (buffer[offset + 1] == 46)
        {
            return ((buffer[offset] - 0x30) + ((buffer[offset + 2] - 0x30) / 10.0)) * (negative ? -1.0 : 1.0);
        }

        //X.XX
        if (buffer[offset + 2] == 46)
        {
            return (((buffer[offset] - 0x30) * 10) + (buffer[offset + 1] - 0x30)) + (((buffer[offset + 3] - 0x30) / 10.0) * (negative ? -1.0 : 1.0));
        }

        return 0.0;
    }
}