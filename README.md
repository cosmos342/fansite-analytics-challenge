# INSIGHT DATA ENGINEERING coding challenge.
* The coding challenge requires to process log.txt input file and produce 4 output files as per the following https://github.com/InsightDataScience/fansite-analytics-challenge </br>
* I have used java for the same with java version of java version "1.8.0_77"
* I have imported java.util,java.lang,java.io,java.time,java.text packages</br>
  * Design </br>
    * Wheel Timer is used to schedule events and timers.</br>
    * Wheel Timer has 300 buckets each bucket has granularity of 1 second</br>
    * 300 buckets chosen to schedule maximum timer of 5 minutes required to block host in case of 3 failed attempts</br>
    * Red black tree implementation based TreeMap is used to store Host entries to keep track of host related events. Provides log(N) search /add/delete times</br>
    * TreeMap is also used to store Resource entries to keep track of bytes consumed for resource </br>
    * PriorityMap are used to keep track of top 10 entries for host(by events),resource(by bytes), hours(by number of events)</br>
    * Also we have Circular Buffer of 3600 entries(for 3600 seconds) to keep track of number of events by each hour.
    * We can make the parameters for wheel timer bucket size, granularity etc to be provided at the run timer as enhancements</br>
  * Optimizations</br>
    * Optimization can be done in re-using the wheel timer buckets instead of allocating and freeing up memory each time an event or timer is added </br>
    * Parsing can be improved to handle all scenarios, this code currently only handles some cases for lack of time to work on the project</br>
  * Debugging</br>
    * A 6th parameter of debug can be given to enable debug. But debug is primitive currently and didn't find time to enhance</br>
    * Another option that is very useful in debugging a software  handling large number of events is circular buffer of event/state history. For example per host state change history can be kept to debug issue with host's state machine bugs.</br>
  * Testing</br>
    * Unit tested with the 4Meg log.txt file provided and verified the program correctly runs and generates output file.
    * For unit testing for each feature i used small files of 10-20 entries to make sure it provides correct output and fixed issues found along the way(such as BLOCK timer not expiring, priority of hosts being wrong and so on)
   

# Credits:
* Thanks for the opportunity. This is a very good challenge and enjoyed working on it.

