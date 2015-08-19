import java.text.SimpleDateFormat

import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}

/**
 * Created by hardik on 16/08/15.
 */
object WeblogChallange extends App {

  val SESSION_TIMEOUT = 900000
  val INPUT_FILE_PATH="/home/hardik/Downloads/WeblogChallenge-master/data/2015_07_22_mktplace_shop_web_log_sample.log.gz"
  val ACCESS_LOG_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

  val sparkConf = new SparkConf().setAppName("WebLogChallange ").setMaster("local[2]")

  //Initialize Spark Context.
  val sc = new SparkContext(sparkConf)



  val logFileLines =  sc.textFile(INPUT_FILE_PATH)

  val dateFormat = new SimpleDateFormat(ACCESS_LOG_DATE_FORMAT)


  val ipKeyLines = logFileLines.map[(String, (Long, Long))](accessLogRow => {
    //Get the time and ip address of access log record.

    //TODO parse URL pages/requests/endpoints

    val rec = accessLogRow.toString()

    /* Parsing date for every record */

    val time = dateFormat.parse(
      rec.substring(0, rec.indexOf('Z')+1)).
      getTime()

    /* Parsing IP for every record */

    val ipSubStr = rec.substring(0 , 66)
    val ipSubStr2 = ipSubStr.substring(ipSubStr.lastIndexOf(':')-16,ipSubStr.lastIndexOf(':'))
    val ipAddress = ipSubStr2.substring(ipSubStr2.lastIndexOf(' ')).trim()

    //Return values from log lines - IP Address followed by startTime, endTime
    (ipAddress,(time,time))

  })

  //Sort IP by timestamp value and pick the earliest time as our sessionStartTime

  val ipSortedByTimeStamp = ipKeyLines.sortBy(_._2)

  var sessionStartTime = ipSortedByTimeStamp.first()._2._1
  var sessionEndTime:Long= sessionStartTime  + SESSION_TIMEOUT
  var sessionId:Int = 0


  //1.Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window

  /*We are sorting the data by time and working with earliest time from log file and using SESSION_TIMEOUT
    to maintain sessionStartTime and endTime - if the event is out of SESSION_TIMEOUT window we will
    treat it as a new session.

    Finally, we emit per session window time spent by each visitor/IP
   */

  val sessionData = ipSortedByTimeStamp.map[((Int,String), (Long,Long,Long))] (a =>{

    //Transform to ((sessionId, IPAddress), (Min start time per session, Maximum end time pe session, duration))
    if(a._2._1 >= sessionStartTime && a._2._1 <= sessionEndTime) {

      ((sessionId,a._1), (a._2._1, a._2._2,0))
    }
    else {
      sessionStartTime = sessionEndTime
      sessionEndTime = sessionStartTime  + SESSION_TIMEOUT
      sessionId = sessionId + 1

      ((sessionId,a._1), (a._2._1, a._2._2,0))

    }
  }).reduceByKey((a, b) => {
    //transform to (sessionId,IpAddress), (lowestStartTime, MaxEndTime))

    (Math.min(a._1, b._1),
      Math.max(a._2, b._2),Math.max(a._2, b._2)-Math.min(a._1, b._1))

  })

  // 2. Average session duration = total duration of all sessions (in seconds) / number of sessions.
  //Calculate total session duration within  each session window

  val durationData = sessionData.map[((Int), (Long))] (a =>{
    //Transform to ((sessionId, totalDuration))
    ((a._1._1), (a._2._3))
  }).reduceByKey((a, b) => {
    //transform to (sessionId,totalDurationWithinSession))
    ((a+b))
  })


  val totalSessions:Long = durationData.count()

  /*Use custom accumulator to hold count values -
   local variables in cluster environment will not produce the same result.*/
  val sessionDurAccumulator = sc.accumulator(0L,"Session Duration Accumulator") (LongAccumulatorParam)

  durationData.foreach(a => sessionDurAccumulator+= a._2)

  //Find duration in seconds
  val durationInSeconds =  (sessionDurAccumulator.value/totalSessions)

  val averageSessionTime = durationInSeconds/totalSessions

  /** *
    * Custom Accumulator: Long Type AccumulatorParam,
    * Spark By Default creates only for Init type.
    *
    */
  object LongAccumulatorParam extends AccumulatorParam[Long] {

    override def addInPlace(r1: Long, r2: Long): Long = {
      r1+r2
    }

    override def zero(initialValue: Long): Long = {
      0L
    }

  }

}