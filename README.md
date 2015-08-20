# WebSessionization
Spark Web sessionization project using scala.

A goal of this project is to process the access-log file and achieve time based sessionization.

Following is the outcome of this project.

1) Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window. 

Output is uploaded to /output1
Format of output1 is

(sessionId, ipAddress), (lowestStartTime, maximumEndTime)

2) Determine the average session time

Output is uploaded to /output2

Format of the output2 is (Labeled Session ID, total duration in milliseconds spend within the session) - handling the average at the end of the program.

3) Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

Output is uploaded to /output3
Format (sessionID, uniqueURL)

====================
Pending items
====================

4) Find the most engaged users, ie the IPs with the longest session times

====================
 Building
====================
1. Please refer to build.sbt
2. Go to project folder in shell - type sbt 
3. run-main WebLogChallange

====================
 Tools and Language 
====================
1. Spark (1.x version)
2. Scala
3. IntelliJ IDE
4. SBT
