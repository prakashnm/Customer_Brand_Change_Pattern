Customer_Brand_Change_Pattern
=============================

Simulating Map-Reduce paradigm to visualize customers brand change and enhance the analysis to build an interest transition graph


Problem Statment:

Input Log File:

User_id Brand Timestamp
1000    brand1 ts4
1005    brand1 ts3
1000    brand2 ts1
1000    brand3 ts3
1005    brand3 ts2
1000    brand4 ts2
1005    brand4 ts1

Deﬁne an "interest pair" to be a pair of consecutive interests one member had activities (ordered by timestamp). Suppose ts1 < ts2 < ts3 < ts4, then we
will have the following pairs:


For 1000, we have: (brand2, brand4), (brand4, brand3), (brand3, brand1)
For 1005, we have: (brand4, brand3), (brand3, brand1)

Expected Output:

(brand2, brand4) 1
(brand4, brand3) 2
(brand3, brand1) 2
.....

