### Object

automate the summarization of the hourly sales of a retailer with an application based on the *Web-Queue-Worker* architecture ([Microsoft, 2021](https://docs.microsoft.com/en-us/azure/architecture/guide/architecture-styles/web-queue-worker)) in a Cloud environment



### Two main components of the application

|       | Client                                                       | Worker                                                       |
| ----- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Def   | web front end that serves client requests                    | performs resource-intensive tasks, long-running workflows, or batch jobs |
| Task1 | reading the CSV file & upload it into the cloud              | wait for a message from the *Client*                         |
| Task2 | send a message to the *Worker* signaling that there is a file ready to be processed | once the message is received with the name of the file to process, read the file |
| Task3 | wait until it receives a message from the *Worker* that the summarization was completed | calculate (a) the **Total Number of Sales**, (b) the **Total Amount Sold** and (c) the **Average Sold** per country and per product |
| Task4 | download the resulting file                                  | write a file in the cloud, and send a message with the name of the file to the Client |
| Task5 |                                                              | wait for another message                                     |

<img src="images/image-20211227203717184.png" alt="image-20211227203717184" style="zoom:50%;" />





### Specification

> <u>Q1:  What type of queues can you create? State their differences</u>
>
> There are two types of Amazon SQS queues: **first-in, first-out (FIFO)** and **standard queues**. 
>
> - In FIFO queues: message strings remain in the same order in which the original messages were sent and received
> - Standard queues : keep message strings in the same order in which the messages were originally sent, but processing requirements may change the original order or sequence of messages
>
> So in our case, it would be better to use <u>FIFO queues</u>.



> <u>Q2: In which situations is a Web-Queue-Worker architecture relevant?</u>
>
> This architecture is commonly used for :
>
> - Applications with a relatively simple domain
> - Applications with some long-running workflows or batch operations
> - When you want to use managed services, rather than infrastructure as a service (IaaS)