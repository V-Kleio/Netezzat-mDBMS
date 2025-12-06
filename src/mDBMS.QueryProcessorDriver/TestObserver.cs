using System;
using System.Collections.Generic;
using System.Linq;

namespace mDBMS.QueryProcessorDriver;

/// <summary>
/// Observes and records the sequence of events during a test run.
/// </summary>
public class TestObserver
{
    private readonly List<string> _actualEvents = new();
    private int _testCount = 0;
    private int _passCount = 0;

    public void Record(string eventDescription)
    {
        _actualEvents.Add(eventDescription);
        Console.WriteLine($"  -> EVENT: {eventDescription}");
    }

    public void Reset()
    {
        _actualEvents.Clear();
    }


    public void Verify(string testName, List<string> expectedEvents)
    {
        _testCount++;
        Console.WriteLine($"\n--- Verifying Test: {testName} ---");
        
        bool success = _actualEvents.SequenceEqual(expectedEvents);

        if (success)
        {
            _passCount++;
            Console.WriteLine("[RESULT] PASS: The actual event sequence matches the expected sequence.");
        }
        else
        {
            Console.WriteLine("[RESULT] FAIL: Event sequence mismatch.");
            Console.WriteLine("Expected Sequence:");
            expectedEvents.ForEach(e => Console.WriteLine($"  - {e}"));
            Console.WriteLine("Actual Sequence:");
            _actualEvents.ForEach(e => Console.WriteLine($"  - {e}"));
        }
    }

    public void PrintSummary()
    {
        Console.WriteLine("\n=====================================");
        Console.WriteLine($"Test Run Summary: {_passCount}/{_testCount} tests passed.");
        Console.WriteLine("=====================================");
    }
}
