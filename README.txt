Node:
- z5

Job: Job1

Objective: To find out state code for entries with country code US and no predefined state codes.

Mapper:

- The Mapper gets the station data and emits 'stations with state code' and 'stations without one' with different integer keys.
- The value simply contains the station information.

Reducer:

- Using hadoop internal mechanism for sort and shuffle, the reducer gets all the entries for these keys in a particular order.
  (List of stations with state ids are processed first and then the list of stations with no predefined state ids.)
- The first list of entries is used to compute the mean longitude for all the entries with state code and store it locally.
- The second list of entries are then processed, where each entry is checked against the mean longitude and partitioned according
  to its location with respect to the mean longitude.
  
Note: Only one reducer is used here as the number of entries are less and to utilize the local storage capacity of the reducer 
      to partition the entries with no state code.

Job: Job2

Data Set Join Strategy:

- Here we are implementing the map side join as one of the data sets (locations data set) is much smaller. 
- It is being stored in the local cache so that all the mappers can load its content in their memory (a local HashMap).
- As the data set is loaded in the memory it improves the performance of the job.

Mapper:

- The Mapper gets the weather data and finds the corresponding state code from the local HashMap.
  (which is populated using the local cache)
- It then emits the <State Code>#<Month Index> as key and <Temperature> (corresponding to the month index) as its value.

Combiner:
- The Combiner gets <State Code>#<Month Index> as key and list of <Temperature> for that particular month as its input (at each node).
- It then calculates the cumulative temperature for that month.
- But as combiner runs on each of the mapper nodes, it also stores the number of entries corresponding to a month.
  (This value will be used to calculate the average temperature at the reducer)
- It then transmits the <State Code> as key and <Month Index>tab<Cumulative Temperature>tab<Number of Entries> as values.

Reducer:

- Reducer will get <State Code> as key and list of <Month Index>tab<Cumulative Temperature>tab<Number of Entries> as values from all the combiners.
- It then computes the average temperature for each month using above data and stores in an array.
- It then calculates the month with the highest average temperature and month with the lowest average temperature and the difference between the two values per state.
- The values computed are stored in a local TreeMap, where difference between the temperature is the key and information corresponding to the record as value.
  (TreeMap stores keys in a sorted manner)
- While cleaning up the reducer writes the required output in ascending order of the difference in temperatures in HDFS.

NOTE: The number of reducers has been set to 1 in job configuration as we expect a very small number of groups (equal to the number of states in US)
	  in the reducer and makes no sense to run a separate job just to sort these entries.

Job: Job2
Objective: To find out which states in the US have the most stable temperature

Data Set Join Strategy:

- Here we are implementing the map side join as one of the data sets (locations data set) is much smaller. 
- It is being stored in the local cache so that all the mappers can load its content in their memory (a local HashMap).
- As the data set is loaded in the memory it improves the performance of the job.

Mapper:

- The Mapper gets the weather data and finds the corresponding station information from the local HashMap.
  (which is populated using the local cache)
- It then emits the <State Code>#<Month Index> as key and <Temperature> (corresponding to the month index) as its value.

Combiner:
- The Combiner gets <State Code>#<Month Index> as key and list of <Temperature> for that particular month as its input (at each node).
- It then calculates the cumulative temperature for that month.
- But as combiner runs on each of the mapper nodes, it also stores the number of entries corresponding to a month.
  (This value will be used to calculate the average temperature at the reducer)
- It then transmits the <State Code> as key and <Month Index>tab<Cumulative Temperature>tab<Number of Entries> as values.

Reducer:

- Reducer will get <State Code> as key and list of <Month Index>tab<Cumulative Temperature>tab<Number of Entries> as values from all the combiners.
- It then computes the average temperature for each month using above data and stores in an array.
- It then calculates the month with the highest average temperature and month with the lowest average temperature and the difference between the two values per state.
- The values computed are stored in a local TreeMap, where difference between the temperature is the key and information corresponding to the record as value.
  (TreeMap stores keys in a sorted manner)
- While cleaning up the reducer writes the required output in ascending order of the difference in temperatures in HDFS.

NOTE: The number of reducers has been set to 1 in job configuration as we expect a very small number of groups (equal to the number of states in US)
	  in the reducer and makes no sense to run a separate job just to sort these entries.

Extra Credit:

Combiner:

- A combiner has been used to reduce number of records transmitted over the network.
- The combiner combines and transmits the output of each mapper to compute the cumulative temperature for each month,
  maintaining the number of entries (which will help the reducer to compute the average accurately).

Enriching Data:

- Average precipitation for the two months has been included in the output.

Station with no state tag:

- I have created two new states namely AO (Atlantic ocean) and PO (Pacific Ocean).
- The job 1 described above goes through the station data and identifies states, where country code is US, which dont have a state tag.
- Using the stations with country code US and predefined state codes we calculated the mean of the longitudes with certain normalizations.
- Now we check the entries without a state code with the mean longitude and partition the states in two state while assigning appropriate state codes (AO or PO).
- 