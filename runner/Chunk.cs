namespace runner;

public struct Chunk
{
    public long From;
    public long To;
    public long Size => To - From + 1;
    
}