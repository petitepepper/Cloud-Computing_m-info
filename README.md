# Cloud Computing Lab2

YE Wenjing,  2021.12





## Instructions

| Task | Java Class Name      | Description                                                  | arg[0]                                | arg[1]                                   | arg[2]                                            |
| ---- | -------------------- | ------------------------------------------------------------ | ------------------------------------- | ---------------------------------------- | ------------------------------------------------- |
| 1    | CreateInstance       | Create an Instance (EC2)                                     | `String` - Instance Name              | `String` - AMI (Amazon Machine Image) id |                                                   |
| 1    | SwitchInstanceStatus | Switch the status (stopped/running) of the given Instance    | `String` - Instance Id                |                                          |                                                   |
| 2    | Lab2Task2Prog1       | - create a bucket<br />- upload a file<br />- create an queue in SQS<br />- send a message to the queue | `String` - bucket name (for creating) | `String` - file name^*^                  |                                                   |
| 2    | Lab2Task2Prog2       | - retrieve message from queue<br />- delete message<br />- retrieve file from S3<br />- [<u>NOT REALIZED</u>^**^] calculate min/max/sum <br />- delete file and bucket | `String` - bucket name (for deletion) | `String` - file name^*^                  | `String` - queue Url (obtain from function above) |

*The file path is `emse/data/values.csv` 

**Difficulty: Not able to deal with the return of `s3.getObject()` . Methods tried: ①  [How can I read an AWS S3 File with Java?](https://stackoverflow.com/questions/28568635/how-can-i-read-an-aws-s3-file-with-java) - [solution don't work]`s3Client.getObject(..)` do not have the `getObjectContent()` method, and its return type is not `S3Object`  ② Use `ResponseTransformer` in `getObject()` : [failed] not able to deal with InputStream (need to transform the `byte[]` of `read()` to `List<Integer>` ) and OutputStream (difficulty in parser csv file) due to the lack of Java knowledge 



## Reference 

[1] [AWS Java SDK - Developer guide](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html)

