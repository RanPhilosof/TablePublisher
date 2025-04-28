using RP.TablePublisherSubscriber;
using System;
using System.Collections.Concurrent;
using System.Threading;

public enum TriggerType
{
    InsertedNewRecord,
    Regular,
    FullPictureToSpecificClients,
    PeriodicIdsAndRevisions,
//    ForceTrigger   // New trigger type to bypass the 200ms condition
}

public class MethodTrigger
{
    public event Action<TriggerType> FireTrigger;

    private bool _isUpdateScheduled = false;
    private readonly Thread _consumerThread;
    private readonly Timer _periodicTimer;
    private readonly Timer _periodicUpdateTimer;
    private readonly ManualResetEvent _eventSignal = new ManualResetEvent(false);
    //private Timer _updateDelayTimer; 
    private int _periodicSyncToClientsTimeSec;
    private int _minTimeBetweenUpdateSents_mSec;

    private NonBlockingCollection<TriggerType> requests = new(); 

    private DateTime _lastUpdateTime;

    private bool isToTriggerUpdateImmediately = false;

    public MethodTrigger(int periodicSyncToClientsTimeSec, int minTimeBetweenUpdateSents_mSec)
    {
        _lastUpdateTime = DateTime.MinValue;

        _periodicSyncToClientsTimeSec = periodicSyncToClientsTimeSec;
        _minTimeBetweenUpdateSents_mSec = minTimeBetweenUpdateSents_mSec;

        _consumerThread = new Thread(ConsumeEvents);
        _consumerThread.Start();

        _periodicTimer = new Timer(PeriodicTask, null, TimeSpan.FromSeconds(_periodicSyncToClientsTimeSec), TimeSpan.FromSeconds(_periodicSyncToClientsTimeSec));
        if (minTimeBetweenUpdateSents_mSec == 0)
            isToTriggerUpdateImmediately = true;
        else
            _periodicUpdateTimer = new Timer(PeriodicUpdateTask, null, minTimeBetweenUpdateSents_mSec, Timeout.Infinite);

        //_updateDelayTimer = new Timer(OnUpdateDelayElapsed, null, Timeout.Infinite, Timeout.Infinite);
    }

    private void PeriodicUpdateTask(object state)
    {
        _periodicUpdateTimer.Change(Timeout.Infinite, Timeout.Infinite);

        try
        {
            _eventSignal.Set();
            //EnqueueEvent(TriggerType.Regular);
        }
        finally
        {
            _periodicUpdateTimer.Change(_minTimeBetweenUpdateSents_mSec, Timeout.Infinite);
        }
    }

    private void PeriodicTask(object state)
    {
        EnqueueEvent(TriggerType.PeriodicIdsAndRevisions);
    }

    public void Inserted()
    {
        EnqueueEvent(TriggerType.InsertedNewRecord);        
    }

    public void Update()
    {
        EnqueueEvent(TriggerType.Regular, isToTriggerUpdateImmediately);        

        //var lastUpdateTime = GetLastSetTime();
        //var now = DateTime.UtcNow;
        //
        ////if (_lastUpdateTime == DateTime.MinValue || (now - _lastUpdateTime).TotalMilliseconds >= _minTimeBetweenUpdateSents_mSec)
        //if (lastUpdateTime == DateTime.MinValue || (now - lastUpdateTime).TotalMilliseconds >= _minTimeBetweenUpdateSents_mSec)
        //{
        //    UpdateTime();
        //    EnqueueEvent(() => Trigger(TriggerType.Regular));            
        //}
        //else
        //{
        //    // If 200ms haven't passed, schedule the trigger for later without blocking
        //    if (!_isUpdateScheduled)
        //    {
        //        _isUpdateScheduled = true;
        //        StartUpdateDelayTimer(_minTimeBetweenUpdateSents_mSec);  // Use timer for non-blocking delay
        //    }
        //}
    }

    // ClientRequests method - triggers immediately
    public void ClientRequests()
    {
        EnqueueEvent(TriggerType.FullPictureToSpecificClients);
    }

    // Method to enqueue events to be processed by the consumer thread
    private void EnqueueEvent(TriggerType triggerType, bool isToSignal = true)
    {
        requests.Enqueue(triggerType);
        //_eventQueue.Enqueue(eventAction);
        if (isToSignal)
            _eventSignal.Set();  // Signal the consumer thread that an event is ready to be processed
    }

    //public enum UrgencyType
    //{
    //    WaitUntilTimeArrives,
    //    NowAndCancelWaitTimeForWaitType,
    //    Now,
    //}

    //Dictionary<TriggerType, UrgencyType> triggersAndUrgency = 
    //    new() {
    //            { TriggerType.Regular, UrgencyType.WaitUntilTimeArrives },
    //            //{ TriggerType.ForceTrigger, UrgencyType.WaitUntilTimeArrives },
    //            { TriggerType.InsertedNewRecord, UrgencyType.NowAndCancelWaitTimeForWaitType },
    //            { TriggerType.PeriodicIdsAndRevisions, UrgencyType.Now },
    //            { TriggerType.FullPictureToSpecificClients, UrgencyType.Now } };

    private int revision = 0;
    // Consumer thread method to process the events
    private bool isDisposed;
    private void ConsumeEvents()
    {
        while (!isDisposed)
        {
            // Wait for a signal that there are events to process
            _eventSignal.WaitOne();

            var req = requests.Dequeue();
            //if (req.Count > 1)
            //    Console.WriteLine();

            var now = DateTime.UtcNow;

            bool isRegularOrInsertedAlreadyPublished = false;

            foreach (var r in req)
            {
                switch (r)
                {
                    case TriggerType.InsertedNewRecord:
                    case TriggerType.Regular:
                        if (!isRegularOrInsertedAlreadyPublished)
                        {
                            Trigger(r);
                            isRegularOrInsertedAlreadyPublished = true;
                        }
                        break;
                    case TriggerType.PeriodicIdsAndRevisions:                        
                    case TriggerType.FullPictureToSpecificClients:
                        Trigger(r);
                        break;
                }                
            }

            _eventSignal.Reset();
        }

        consumeEventsStoped.Set();
    }
    
    private readonly ManualResetEvent consumeEventsStoped = new ManualResetEvent(false);

    private void StopConsumeEvents()
    {
        isDisposed = true;
        _eventSignal.Set();
        consumeEventsStoped.WaitOne();
    }

    // Trigger method - simulate triggering action
    private void Trigger(TriggerType triggerType)
    {
        //bool isToTrigger = true;
        //
        //var lastUpdateTime = GetLastSetTime();
        
        ////if (lastUpdateTime != DateTime.MinValue 
        ////    &&
        ////    triggerType == TriggerType.ForceTrigger           
        ////    &&
        ////    (DateTime.UtcNow - lastUpdateTime).TotalMilliseconds < _minTimeBetweenUpdateSents_mSec * 0.95)
        ////{
        ////    isToTrigger = false;            
        ////    StartUpdateDelayTimerConsumerContext(_minTimeBetweenUpdateSents_mSec); 
        ////}

        //bool isToTrigger = false;
        // Update _lastUpdateTime only for Regular or InsertedNewRecord
        //if (triggerType == TriggerType.Regular || triggerType == TriggerType.InsertedNewRecord || triggerType == TriggerType.ForceTrigger)
        //{
        //    if (triggerType == TriggerType.InsertedNewRecord || _lastUpdateTime == DateTime.MinValue || (DateTime.UtcNow - _lastUpdateTime).TotalMilliseconds >= _minTimeBetweenUpdateSents_mSec * (triggerType == TriggerType.ForceTrigger ? 0.95 : 1.0))
        //    {
        //        //if (triggerType == TriggerType.ForceTrigger)
        //        //{
        //        //    var time = (DateTime.UtcNow - _lastUpdateTime).TotalMilliseconds;
        //        //}
        //
        //        isToTrigger = true;
        //        _lastUpdateTime = DateTime.UtcNow;
        //    }
        //}
        //else
        //{
        //    isToTrigger = true;
        //}

        ////if (isToTrigger)
            FireTrigger?.Invoke(triggerType);
            //Console.WriteLine($"Triggering: {triggerType} at {DateTime.UtcNow}");
    }

    // Method to start the delayed trigger using a Timer
    //private void StartUpdateDelayTimer(int delayMilliseconds)
    //{
    //    // Use a Timer to handle the delay without blocking
    //    _updateDelayTimer.Change(delayMilliseconds, Timeout.Infinite);
    //}

    ////private void StartUpdateDelayTimerConsumerContext(int delayMilliseconds)
    ////{
    ////    // Use a Timer to handle the delay without blocking
    ////    _updateDelayTimerConsumerContext.Change(delayMilliseconds, Timeout.Infinite);
    ////}

    // Callback for the timer when the delay has elapsed
    //private void OnUpdateDelayElapsed(object state)
    //{
    //    // Always trigger Regular after 200ms delay (from Update method)
    //    _isUpdateScheduled = false;
    //    //EnqueueEvent(() => Trigger(TriggerType.Regular)); // Trigger Regular after delay
    //
    //    // Optionally trigger ForceTrigger as well (or based on some condition)
    //    EnqueueEvent(TriggerType.ForceTrigger); // This will trigger immediately after delay, bypassing the normal 200ms check
    //}
    //
    //private void OnUpdateDelayElapsedConsumerContext(object state)
    //{
    //    EnqueueEvent(TriggerType.ForceTrigger);
    //}

    // Stop the periodic task if needed
    public void StopPeriodicTask()
    {
        _periodicTimer.Dispose();
        _periodicUpdateTimer.Dispose();
        StopConsumeEvents();
    }
}