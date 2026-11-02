Using MapReducer to process SPY_May_2012.csv :<br>
(1) Use only the traded quantities and their corresponding prices. ignore quote.<br>
(2) For intraday day, combine trading quantities into each 15min interval according to the timestamp of each trade.<br>
(3) For the quantity of each 15 min interval (NYC Local Time, 4 hours after Greenwich Time(GMT)), sum up the corresponding quantity for each trading day from 5/1/12 to 5/20/12 and then calculate the average for each time interval by dividing the sum by the number of trading days, shown in the output table.<br>
(4) For the execution prices, use the trading prices on 5/21/12. Assume the price of 1st trade in each time interval as the execution price for the entire interval, see the table of the next slide.<br>

