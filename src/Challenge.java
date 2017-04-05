// Open the file
import java.util.*;
import java.lang.*;
import java.io.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.text.*;


// Entry in Wheel stores log events or timers
class WheelEntry
{
	public enum EntryType { NONE,LOG_EVENT, FAILLOGIN_TIMER, BLOCK_TIMER };
	String event;
	Host  host;
	EntryType type;
	int lineNum;
	int wheelIdx;
	WheelEntry()
	{
		type = EntryType.NONE;
		host = null;
		event = null;
	}
}

// Host. Has information about  host and number of
// times host has contacted server
class  Host implements Comparable
{
	String myName;
	long numReqs;
	int numFailedAttempts;
	Boolean inFreqQueue;
	static Challenge challg;

	static final int FAILED_LOGIN_CODE = 401;
	static final int SUCCESSFUL_LOGIN_CODE = 200;
	static final int FAILLOGIN_TIMEOUT = 20;
	static final int BLOCKLOGIN_TIMEOUT = 300;

	public enum HOST_STATE { HOST_OK, HOST_FAILLOGIN, HOST_INBLOCK };
	
	HOST_STATE state;
	int numFailedLogin;
	WheelEntry timer;

	Host(String host)
	{
		myName = host;
		numReqs=1;
		inFreqQueue = false;
		numFailedLogin=0;
	
		state = HOST_STATE.HOST_OK;
	}

	void startTimer()
	{
		timer = new WheelEntry();
		timer.host = this;
		if(state == HOST_STATE.HOST_FAILLOGIN)
		{
			timer.type = WheelEntry.EntryType.FAILLOGIN_TIMER;
			challg.enqueTimer(timer,FAILLOGIN_TIMEOUT);
		}
		else if(state ==  HOST_STATE.HOST_INBLOCK)
		{
			timer.type = WheelEntry.EntryType.BLOCK_TIMER;
			challg.enqueTimer(timer,BLOCKLOGIN_TIMEOUT);
		}
	}

	void stopTimer()
	{
		challg.dequeTimer(timer);
	}

	void processTimeout()
	{
		if((state == HOST_STATE.HOST_FAILLOGIN) ||
		   (state == HOST_STATE.HOST_INBLOCK))
		{
			numFailedLogin = 0;
			state = HOST_STATE.HOST_OK;
			// DEBUG("block timer expiration");
		}
		else
		{
			// DEBUG("timeout Expiration in the HOST_STATE.HOST_OK state wrong");
		}
		
	}

	void  logHostBlockEvent(String event)
	{
		// log to file
		// DEBUG("log block event" + event);
		challg.logHostBlockEvent(event);
	}

	void processFailedLogin()
	{
		if(state == HOST_STATE.HOST_OK)		
		{
			// First Attempt start timer failtimer
			numFailedLogin = 1;
			state = HOST_STATE.HOST_FAILLOGIN;
			startTimer();
			// DEBUG("start fail timer");

		}
		else if(state == HOST_STATE.HOST_FAILLOGIN)
		{
			numFailedLogin++;
			if(numFailedLogin == challg.getMaxNumFailedLogins())
			{
				// cancel failtimer and start block timer
				stopTimer();
				state = HOST_STATE.HOST_INBLOCK;
				startTimer();
				// DEBUG("start block timer");
			}
		}
		else
		{
			challg.DEBUG("processFailedLogin called in BLOCKED STATE not handled");
		}
	}

	void processLoginSucc()
	{
		// cancel faillogin timer
		if(state == HOST_STATE.HOST_FAILLOGIN)
		{
			numFailedLogin = 0;
			state = HOST_STATE.HOST_OK;
			stopTimer();
			// DEBUG("succ loging stop timer");
		}
	}
	
	void processEvent(String event)
	{
		// log to file
		// DEBUG("PHOST " + event);
		if(state == HOST_STATE.HOST_INBLOCK)
		{
			// DEBUG("processing event in blocked state");
			logHostBlockEvent(event);
		}
		else
		{
			// check if event is failure event or succ event
			String [] elements = event.split("\\s+");
			// DEBUG("processEvent " + event);
			// DEBUG("processEvent element lenght " + elements.length);
			if(Challenge.isInteger(elements[elements.length-2]) == true)
			{
				int code = Integer.parseInt(elements[elements.length-2]);
				if(code  ==  FAILED_LOGIN_CODE)
				{
					processFailedLogin();	
				}
				else if((state == HOST_STATE.HOST_FAILLOGIN) && code == SUCCESSFUL_LOGIN_CODE)
				{
					processLoginSucc();
				}
				else 
				{
					// todo is any code not 401 a successful login?
				}
			}
		}
	}

	void incrReq()
	{
		numReqs++;
	}
	

	public int compareTo(Object o)
	{
		Host other = (Host)o;
		if(numReqs > other.numReqs)
			return 1;
		else if(numReqs < other.numReqs)
			return -1;
		// add lexicographic ordering here
		else
		{ 
			if(myName.compareTo(other.getName()) > 0)
				return 1;
			else if(myName.compareTo(other.getName()) < 0)
				return -1;
			else
				return 0; // should not happen if you are comparing to others
		}
	}

	long getNumReq()
	{
		return numReqs;
	}

	Boolean getInFreqQueue()
	{
		return inFreqQueue;
	}
	void setInFreqQueue(Boolean flag)
	{
		inFreqQueue = flag;
	}

	String getName()
	{
		return myName;
	}
}

// Stores EventsPerHour and this entry stored in priority Q
// To get top 10 hours
class EventsPerHour implements Comparable
{
	long numEvents;
	// long startTimeTick; TBD optimization
	long startEpoch;

	EventsPerHour(long inNumEvents, long startTime)
	{
		numEvents = inNumEvents;
		// startTimeTick = inTimeTick;
		startEpoch = startTime;
	}

	public int compareTo(Object o)
	{
		EventsPerHour other = (EventsPerHour)o;
		if(numEvents > other.numEvents)
		{
			return 1;
		}
		else if(numEvents < other.numEvents)
		{
			return -1;
		}
		else 
		{
			// add lexicographic ordering
			if(startEpoch < other.startEpoch)
			{
				return 1;
			}
			else if(startEpoch > other.startEpoch)
			{
				return -1;
			}
			else
				return 0; // shouldn't happen if u are comparing other
		}
	}
}


// Stores resource name and number of bytes the resource has used
class  Resource implements Comparable
{
	String myName;
	long numBytes;
	Boolean inBytesQueue;

	Resource(String host,long bytes)
	{
		myName = host;
		numBytes=bytes;
		inBytesQueue = false;
	}
	void incrBytes(long bytes)
	{
		numBytes += bytes;
	}
	

	public int compareTo(Object o)
	{
		Resource other = (Resource)o;
		if(numBytes > other.numBytes)
			return 1;
		else if(numBytes < other.numBytes)
			return -1;
		// add lexicographic ordering here
		else
		{ 
			if(myName.compareTo(other.getName()) > 0)
				return 1;
			else if(myName.compareTo(other.getName()) < 0)
				return -1;
			else
				return 0; // should not happen if you are comparing to others
		}

	}

	long getNumBytes()
	{
		return numBytes;
	}

	Boolean getInBytesQueue()
	{
		return inBytesQueue;
	}

	void setInBytesQueue(Boolean flag)
	{
		inBytesQueue = flag;
	}

	String getName()
	{
		return myName;
	}
}

class Challenge
{
	// PRIORITY QUEUE TO TRACK TOP 10 HOSTS
	private PriorityQueue<Host> hostByReq;

	// PRIORITY QUEUE TO TRACK TOP 10 RESOURCE
	private PriorityQueue<Resource> resByBytes;

	// PRIORITY QUEUE TO TRACK TOP 10 HOURS
	private PriorityQueue<EventsPerHour> evtsPerHr;


	// STORES ALL HOSTS
	private TreeMap<String,Host> hostTree;

	// STORES ALL RESOURCE
	private TreeMap<String,Resource> resTree;


	// STORES UPTO 3600 rolling eventspersec
	private ArrayList<Long> eventsPerSec;


	// THIS IS WHEEL TIMER WHICH can SUPPORT storing timer for max timer of 300 seconds
	private ArrayList<ArrayList<WheelEntry>> wheel;

	private int curWheelIdx;
	private int numEventsInWheel; // count only log events no timer events
	private int curEventsPerSecIdx;
	private int curEventsPerHourCount;
	private BufferedReader br;

	private BufferedWriter wrHost;
	private BufferedWriter wrRes;
	private BufferedWriter wrHr;
	private BufferedWriter wrBlked;

	private int count404;
	private int countOther;
	private int countTotal;
	private Long curEpoch;

	private Boolean isEventPending;
	private String pendingEvent;
	private int pendingLineNum;
	private Long pendingEpoch;
	private int maxNumFailedLogins;

	private final int NUM_EVENTS_PER_SECOND_ENTRIES = 3600;
	private final int NUM_WHEEL_BUCKETS = 300;
	private final int TIME_OFFSET = 3;
	private final long TICKS_PER_S = 1000;

	private Boolean pastFirstHour;

	private Boolean debugEnable;


	Challenge(int inMaxNumFailedLogins,String [] args)
	{

		hostByReq = new PriorityQueue<>();
		resByBytes = new PriorityQueue<>();
		evtsPerHr = new PriorityQueue<>();
		hostTree = new TreeMap<>();
		resTree = new TreeMap<>();
		eventsPerSec = new ArrayList<>(NUM_EVENTS_PER_SECOND_ENTRIES);
		for (int i = 0 ; i < NUM_EVENTS_PER_SECOND_ENTRIES ; i++)
		{
			eventsPerSec.add(0L);
		}
		curWheelIdx=0;
		numEventsInWheel=0;
		curEventsPerSecIdx=0;
		curEventsPerHourCount = 0;
		count404=0;
		countOther=0;
		countTotal=0;
		FileInputStream fstream = null;
		isEventPending=false;
		maxNumFailedLogins = inMaxNumFailedLogins;
		pastFirstHour = false;
		debugEnable=false;


		wheel = new ArrayList<>(NUM_WHEEL_BUCKETS); 
		{
			for(int i = 0 ; i < NUM_WHEEL_BUCKETS ; i++)
			{
				ArrayList<WheelEntry> wRow = new ArrayList<>(10);
				wheel.add(wRow);
				// TODO
				// for(int i = 0 ; i < 10 ; i++)
				// {
				// 	WheelEntry wE = new WheelEntry();
				// 	wRow.add(wE);
				// }
			}
		}

		// READER
		try {
			fstream = new FileInputStream(args[0]);
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}

		try {
			br = new BufferedReader(new InputStreamReader(fstream));
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}

		// WRITERS

		try {
			wrHost = new BufferedWriter(new OutputStreamWriter( new FileOutputStream(args[1]), "utf-8"));
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}


		try {
			wrHr = new BufferedWriter(new OutputStreamWriter( new FileOutputStream(args[2]), "utf-8"));
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}

		try {
			wrRes = new BufferedWriter(new OutputStreamWriter( new FileOutputStream(args[3]), "utf-8"));
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}

		try {
			wrBlked = new BufferedWriter(new OutputStreamWriter( new FileOutputStream(args[4]), "utf-8"));
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}
	
		if(args.length == 6 && args[5].equals("debug"))
		{
			debugEnable=true;
		}
	}
	
	public void DEBUG(String s)
	{
		if(debugEnable  == true)
			System.out.println(s);
	}

	public static boolean isInteger(String s) {
    		try { 
        		Integer.parseInt(s); 
    		} catch(NumberFormatException e) { 
       			return false; 
    		} catch(NullPointerException e) {
        		return false;
    		}
    		// only got here if we didn't return false
    		return true;
	}

	
	// ENQUE HOST TIMER FOR FAILED LONGIN and BLOCKED TIMER	
	void enqueTimer(WheelEntry timer,int num_seconds)
	{
		if(num_seconds <= NUM_WHEEL_BUCKETS)
		{
			int wheelIdx = (curWheelIdx+num_seconds-1)%NUM_WHEEL_BUCKETS;
			// DEBUG("enqueTimer timer " + num_seconds + " curIdx " + curWheelIdx + " newIdx " + wheelIdx ); 
			timer.wheelIdx = wheelIdx;
			ArrayList<WheelEntry> bucket = wheel.get(wheelIdx);
			bucket.add(timer);
		}
		else
		{
			DEBUG("ENQUEUE TIMER timer is big need to handle this ");
		}
	}

	// CANCEL HOST TIMER FOR FAILED LONGIN and BLOCKED TIMER	
	void dequeTimer(WheelEntry timer)
	{
		ArrayList<WheelEntry> bucket = wheel.get(timer.wheelIdx);
		// DEBUG("DEQUE TIMER timer is removed index " + timer.wheelIdx);
		bucket.remove(timer);
	}

	int getMaxNumFailedLogins()
	{
		return	maxNumFailedLogins;
	}

	Long utilTimeToEpoch(String s)
	{	
		Long epoch = 0L;
		try {
			String eventTime = s.replaceAll("\\[","").replaceAll("\\]","");
        		DateTimeFormatter dtf  = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss X");
        		ZonedDateTime     zdt  = ZonedDateTime.parse(eventTime,dtf);        
			epoch = zdt.toInstant().toEpochMilli();
			// DEBUG("utilTimeToEpoch epoch " + epoch);
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}
		return epoch;
	}

 	Boolean initEpoch(String s)
	{
		String [] elements = s.split("\\s+");
		if(elements.length > 4)
		{
			curEpoch = utilTimeToEpoch(elements[TIME_OFFSET]+ " " + elements[4]);
			DEBUG("initialized epoch " + curEpoch);
		}
		else
		{
			DEBUG("initEpoch message size is less than 3 can't handle");
			return false;
		}
		return true;
	}

	public static boolean isHTTPMessage(String s)
	{
		if(s.contains("GET") || s.contains("POST") || s.contains("HEAD") || s.contains("PUT") || s.contains("DELETE")
			|| s.contains("TRACE") || s.contains("OPTIONS") || s.contains("CONNECT") || s.contains("PATCH"))
			return true;
		return false;

	}

	// Add HOST TO PRIORITY QUEUE IF EvENTS ARE IN TOP 10 
	void addHostsByFreq(Host hostInst)
	{
		if(hostInst.getInFreqQueue() == false)
		{
			if(hostByReq.size() < 10)
			{
				hostByReq.offer(hostInst);
				hostInst.setInFreqQueue(true);
			}
			else if(hostInst.compareTo(hostByReq.peek()) > 0)
			{
				Host outHostInst = hostByReq.poll();
				hostByReq.offer(hostInst);
				outHostInst.setInFreqQueue(false);
				hostInst.setInFreqQueue(true);
			}
		}
		else
		{
			// have to remove and readd to adjust position
			hostByReq.remove(hostInst);
			hostByReq.offer(hostInst);
		}
	}

	// Add RESOURCE TO PRIORITY QUEUE IF BYTES ARE IN TOP 10
	void addResourceByBytes(Resource resInst)
	{
		if(resInst.getInBytesQueue() == false)
		{
			if(resByBytes.size() < 10)
			{
				resByBytes.offer(resInst);
				resInst.setInBytesQueue(true);
			}
			else if(resInst.compareTo(resByBytes.peek()) > 0)
			{
				Resource outResInst = resByBytes.poll();
				resByBytes.offer(resInst);
				outResInst.setInBytesQueue(false);
				resInst.setInBytesQueue(true);
			}
		}
		else
		{
			// have to remove and readd to adjust position
			resByBytes.remove(resInst);
			resByBytes.offer(resInst);
		}
	}

	void processHost(String host, String event)
	{
		Host hostInst = null;
		if(hostTree.containsKey(host))
		{
			hostInst = hostTree.get(host);
			hostInst.incrReq();
		}
		else
		{
			hostInst = new Host(host);
			hostTree.put(host,hostInst);
		}

		addHostsByFreq(hostInst);
		hostInst.processEvent(event);
	}

	void processResource(String res, long bytes)
	{
		Resource resInst = null;
		if(resTree.containsKey(res))
		{
			resInst = resTree.get(res);
			resInst.incrBytes(bytes);
		}
		else
		{
			resInst = new Resource(res,bytes);
			resTree.put(res,resInst);
		}

		addResourceByBytes(resInst);
	}

	void printHostByReq()
	{
		Object [] hosts = hostByReq.toArray();
		Arrays.sort(hosts,Collections.reverseOrder());
		for (Object objInst :hosts ) {

			Host hostInst = (Host)objInst;
			DEBUG("HOST " + hostInst.myName +  "  FREQ " + hostInst.getNumReq());
    			try {
				wrHost.write(hostInst.myName + "," + hostInst.getNumReq() +  "\n");
			}
			catch(Exception e)
			{
				System.err.println(e.getMessage());
			}
		}
	}

	void printResourceByBytes()
	{
		Object [] ress = resByBytes.toArray();
		Arrays.sort(ress,Collections.reverseOrder());
		for (Object objInst :ress) {

			Resource resInst = (Resource)objInst;
			DEBUG("RESOURCE " + resInst.myName +  "  BYTES " + resInst.getNumBytes());
			try {
				wrRes.write(resInst.myName + "\n");
			}
			catch(Exception e)
			{
				System.err.println(e.getMessage());
			}
		}
	}

	String utilEpochToTime(long startEpoch,String zone)
	{
		Date theDate = new Date(startEpoch);
		DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
		df.setTimeZone(TimeZone.getTimeZone(zone));
		String str = df.format(theDate);
		// DEBUG(str);
		return str;
	}

	void printHoursByEvents()
	{

		Object [] evts = evtsPerHr.toArray();
		Arrays.sort(evts,Collections.reverseOrder());
		for (Object objInst : evts) {

			EventsPerHour evtInst = (EventsPerHour)objInst;
			String time = utilEpochToTime(evtInst.startEpoch,"US/Eastern");
			DEBUG("HOUR " + time +  ", NUMEVENTS " + evtInst.numEvents);
			try {
				wrHr.write(time + "," + evtInst.numEvents + "\n");
			}
			catch(Exception e)
			{
				System.err.println(e.getMessage());
			}
		}
	}

	void  logHostBlockEvent(String event)
	{
		try {
			wrBlked.write(event + "\n");
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}
	}

	void processWheelEntry(WheelEntry we)
	{
		// PROCESS LOG EVENTS
		if(we.type == WheelEntry.EntryType.LOG_EVENT)
		{
			// Host hostInst = null;
			String [] elements = we.event.split("\\s+");
			processHost(elements[0],we.event);
			if(elements.length >= 6 && isHTTPMessage(elements[5]))
			{
				if(isInteger(elements[elements.length-1]))
				{
					processResource(elements[6],Integer.parseInt(elements[elements.length-1]));
				}
				else
				{
					count404++;
				}
			}
			else
			{
				if(countOther == 0)
				{
					DEBUG("COUNT OTHER MISS line# " + we.lineNum);
					for(String entry : elements)
					{
							DEBUG(entry);
					}
				}
				countOther++;
			}
					
		}
		else
		{
			// PROCESS TIMER EXPIRATIONS
			// DEBUG("Timer expiration curEpoch " + curEpoch + " curIdx " + curWheelIdx);
			we.host.processTimeout();
		}
	}

	Long utilGetEpoch(String event)
	{
		String [] elements = event.split("\\s+");
		Long epoch = utilTimeToEpoch(elements[TIME_OFFSET]+ " " + elements[4]);
		return epoch;
	}

	Long utilGetWheelIndexOffSetFromEpoch(Long epoch)
	{
		return (epoch-curEpoch)/TICKS_PER_S;
	}


	Long utilGetWheelIndexOffSet(String event)
	{
		Long epoch = utilGetEpoch(event);
		// DEBUG("utlGetWheel " + " curEpoch " + curEpoch + " eventEpoch " + epoch );
		return utilGetWheelIndexOffSetFromEpoch(epoch);
	}


	void utilUpdateEventsPerHour(Long hourStartTime)
	{
		// DEBUG("updating events per hour entry");
		EventsPerHour eHrEntry = new EventsPerHour(curEventsPerHourCount, hourStartTime);
		if(evtsPerHr.size() < 10)
		{
			evtsPerHr.offer(eHrEntry);
		}
		else if(evtsPerHr.peek().compareTo(eHrEntry) < 0)
		{
			evtsPerHr.poll();	
			evtsPerHr.offer(eHrEntry);
		}
		
		// reduce the eventsPerHourCount by leading entry value;
	}

	void utilUpdateCurWheelIdx()
	{
		// update the events per sec index
		curEventsPerSecIdx = (curEventsPerSecIdx+1)%NUM_EVENTS_PER_SECOND_ENTRIES; 
		if(curEventsPerSecIdx == 0)
		{
			pastFirstHour = true;
			// if pastFirstHour then we can start logging hourly entries
		}

		if(pastFirstHour == true)
		{
	 		Long hourStartTime = curEpoch - (NUM_EVENTS_PER_SECOND_ENTRIES*TICKS_PER_S);
			utilUpdateEventsPerHour(hourStartTime);
		}
		// update the events per hour
		curEventsPerHourCount -= eventsPerSec.get(curEventsPerSecIdx);
		eventsPerSec.set(curEventsPerSecIdx,0L);
		// update the curEpoch
		curEpoch += TICKS_PER_S;
		// update the currentwheel index.
		curWheelIdx = (curWheelIdx+1)%NUM_WHEEL_BUCKETS;
	}

	void utiladdEventToWheel(int wheelIdxOffSet,String event,int lineNum)
	{
		int wheelIdx = (curWheelIdx+wheelIdxOffSet)%NUM_WHEEL_BUCKETS;
		// DEBUG("addEvent cur " + curWheelIdx + " wheelIdx " + wheelIdx);
		// DEBUG(event);
		ArrayList<WheelEntry> bucket = wheel.get(wheelIdx);
		WheelEntry we = new WheelEntry();
		we.type = WheelEntry.EntryType.LOG_EVENT;
		we.event = event;
		we.lineNum = lineNum;
		bucket.add(we);
		numEventsInWheel++;
	}
	

	// ADD LOG EVENT TO WHEEL AT  RIGHT BUCKET
	boolean addEventToWheel(String event, int lineNum)
	{
		// add to wheel in right bucket
		Long myoffset = utilGetWheelIndexOffSet(event);
		int wheelIdxOffSet = (utilGetWheelIndexOffSet(event)).intValue();
		// DEBUG("addEventToWheel offset Long " + myoffset + " int " + wheelIdxOffSet);
		
		// DEBUG(" addEventToWheel nextEventOffset " + wheelIdxOffSet);
		if(wheelIdxOffSet <= NUM_WHEEL_BUCKETS)
		{
			
			utiladdEventToWheel(wheelIdxOffSet,event,lineNum);
			// DEBUG("addEventToWheel added #events " + numEventsInWheel + " bucket " + wheelIdx);
			return true;
		}
		else
		{
			if(isEventPending == false)
			{
				isEventPending=true;
				pendingEvent = event;
				pendingLineNum = lineNum;
				pendingEpoch = utilGetEpoch(pendingEvent);
				// DEBUG("NEED TO HANDLE THIS PROCESS ALL EVENTS IN WHEEL BEFORE ADMITTING NEW retuned wheel idx " + wheelIdxOffSet + " Line# " + lineNum);
				// DEBUG(event);
			}
			return false;
		}
	}

	void utilIncrementEventsPerSec()
	{
		eventsPerSec.set(curEventsPerSecIdx, (eventsPerSec.get(curEventsPerSecIdx)+1));
		curEventsPerHourCount++;
	}

	boolean queueNextEventPending()
	{
		if(isEventPending == true)
		{
			int wheelIdxOffSet =  utilGetWheelIndexOffSetFromEpoch(pendingEpoch).intValue();
			if(wheelIdxOffSet < NUM_WHEEL_BUCKETS)
			{
				utiladdEventToWheel(wheelIdxOffSet,pendingEvent,pendingLineNum);
				isEventPending=false;
				// DEBUG("QUEUED PENDING EVENT " + " line# " + pendingLineNum);
				return true;
			}
		}
		return false;
	}

	// SCHEDULE EVENTS/TIMERS at CURRENT TIME
	void scheduleWheel()
	{
		ArrayList<WheelEntry> bucket  = wheel.get(curWheelIdx);
		// need to make this WheelEntries reusable.
		while(((isEventPending == true)  && (queueNextEventPending() == false)) ||
			(numEventsInWheel > 0)) 
		{
			if(bucket.size() == 0)
			{
					utilUpdateCurWheelIdx();
					bucket  = wheel.get(curWheelIdx);
			}

			while(bucket.size() > 0)
			{
				WheelEntry we = bucket.remove(0);
				if(we.type == WheelEntry.EntryType.LOG_EVENT)
				{
					utilIncrementEventsPerSec();
					numEventsInWheel--;
				}
				processWheelEntry(we);
			}
		}
	}

	// GET EVENTS FROM LOGFILE 
	Boolean dequeEvents()
	{	
		long numEvents =0;
		String strLine = null;

		//Read File Line By Line
		while (numEvents < 10) 
		{
			numEvents++;
			try {
				if((strLine = br.readLine()) != null)
				{
					if(countTotal == 0)			
					{
						// initialize curEpoch
						boolean succ = initEpoch(strLine);
						if(succ == true)
						{
							if(addEventToWheel(strLine,countTotal) == false)
							{
								countTotal++;
								break;
							}
						}
					}
					else
					{
						if(addEventToWheel(strLine,countTotal) == false)
						{
							countTotal++;
							break;
						}
					}
					countTotal++;
				}
				else
				{
					try {
						br.close();
					}
					catch(Exception e)
					{
						System.err.println(e.getMessage());
	
					}
					return false;
				}
			}
	
			catch(Exception e)
			{
				System.err.println(e.getMessage());
			}

		}
		return true;
	}


	void utilCloseWriters()
	{
		try {
			wrHost.close();
		}
		catch(Exception e)	
		{
			System.err.println(e.getMessage());
		}
		
		try {
			wrRes.close();
		}
		catch(Exception e)	
		{
			System.err.println(e.getMessage());
		}

		try {
			wrHr.close();
		}
		catch(Exception e)	
		{
			System.err.println(e.getMessage());
		}

		try {
			wrBlked.close();
		}
		catch(Exception e)	
		{
			System.err.println(e.getMessage());
		}
	}

	void accountEventsInPastHour()
	{
		// get time one hour ago;
		int startEvtPerSecIdx;
		Long hourStartTime;
		if(pastFirstHour == false)
		{
			hourStartTime = curEpoch - (curEventsPerSecIdx)*TICKS_PER_S;
			startEvtPerSecIdx = 0;
		}
		else 
		{
			hourStartTime = curEpoch -(NUM_EVENTS_PER_SECOND_ENTRIES*TICKS_PER_S);
			startEvtPerSecIdx = (curEventsPerSecIdx+1)%NUM_EVENTS_PER_SECOND_ENTRIES;
			
		}

		while(startEvtPerSecIdx != curEventsPerSecIdx)
		{
			utilUpdateEventsPerHour(hourStartTime);
			curEventsPerHourCount -= eventsPerSec.get(startEvtPerSecIdx);
			eventsPerSec.set(curEventsPerSecIdx,0L);
			startEvtPerSecIdx = (startEvtPerSecIdx+1)%NUM_EVENTS_PER_SECOND_ENTRIES; 
			// update the curEpoch
			hourStartTime += TICKS_PER_S;
		}
		utilUpdateEventsPerHour(hourStartTime);
		curEventsPerHourCount -= eventsPerSec.get(startEvtPerSecIdx);
		DEBUG("accountEventsInPastOur. RemainingEvents " + curEventsPerHourCount);

	}

	// READ EVENTS AND SCHEDULE WHEEL
	void mainLoop()
	{
		while (true)
		{
			if(dequeEvents() == false)
			{
				// done queueing all events
				scheduleWheel();

				accountEventsInPastHour();
				printHostByReq();
				printResourceByBytes();
				printHoursByEvents();
				utilCloseWriters();
				DEBUG("Missed number of lines for bytes " + "404 " + count404 +  " OTHER " + countOther);
		
				break;
			}
			else
			{
				scheduleWheel();
			}
		}
	}

	public static void main(String [] args)
	{
		FileInputStream fstream = null;
		BufferedReader br = null;
		int maxNumFailedLogins = 3;
		if(args.length != 5 &&  args.length != 6)
		{
			System.out.println("Error: provide 5 arguments in the form of full paths for log, host,hours,resources,blocked files respectively ");
			System.out.println("Error: provide 6 arguments in the form of paths for log, host,hours,resources,blocked files respectively followed by debug keyword to enable debugs ");
			
		}
		else
		{
			Challenge challg = new Challenge(maxNumFailedLogins,args);
			Host.challg = challg;
		
			challg.mainLoop();
		}
	}
}
