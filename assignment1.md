Question 1.
In this assignment, I use 2 Mappers and 2 Reducers.
We use Shakespeare.txt collection as the source.Tokenizer function is used to token each word from text. 
For both PairsPMI and StripesPMI,the intermediate key-value pair is key and (word,1), whch means the word in a line appears or not.So the intermediate file records the count of each word appears in whole Shakespeare.txt.
For PairsPMI, the format of output records is (a,b)(PMI, count)
For StripesPMI, the format of output records is (a,(b(PMI,count)))




Question 2.
What is the running time of the complete pairs implementation?
Job Finished in 53.629 seconds

What is the running time of the complete stripes implementation? 
Job Finished in 71.776 seconds
linux.student.cs.uwaterloo.ca or 



Question 3. (2 points) Now disable all combiners. What is the running time of the complete pairs implementation now? 
Job Finished in 66.617 seconds

What is the running time of the complete stripes implementation? 
Job Finished in 100.914 seconds
linux.student.cs.uwaterloo.ca 

Question 4. 
308792


Question 5. 
highest PMI:
(maine, anjou)	(3.6331422, 12)
(anjou, maine)	(3.6331422, 12)

lowest PMI:
(thy, you)	(-1.5303967, 11)
(you, thy)	(-1.5303967, 11)



Question 6.
'tears':
(tears, shed)	(2.1117902, 15)
(tears, salt)	(2.052812, 11)
(tears, eyes)	(1.165167, 23)

'death':
(death, father's)	(1.120252, 21)
(death, die)	(0.7541594, 18)
(death, life)	(0.7381346, 31)



Question 7.
(hockey, defenceman)	(2.4031334, 147)
(hockey, winger)	(2.3864822, 185)
(hockey, goaltender)	(2.2435493, 198)
(hockey, ice)	(2.1956174, 2002)
(hockey, nhl)	(1.9857546, 937)


Question 8.
(data, storage)	(1.9805377, 100)
(data, database)	(1.9001268, 97)
(data, disk)	(1.7944009, 67)
(data, stored)	(1.7885251, 65)
(data, processing)	(1.6491873, 57)

