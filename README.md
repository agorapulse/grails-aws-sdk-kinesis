Grails AWS SDK Kinesis Plugin
=============================

[![Build Status](https://travis-ci.org/agorapulse/grails-aws-sdk-kinesis.svg?token=BpxbA1UyYnNoUwrDNXtN&branch=master)](https://travis-ci.org/agorapulse/grails-aws-sdk-kinesis)

# Introduction

The AWS SDK Plugins allow your [Grails](http://grails.org) application to use the [Amazon Web Services](http://aws.amazon.com/) infrastructure services.
The aim is to provide lightweight utility Grails service wrappers around the official [AWS SDK for Java](http://aws.amazon.com/sdkforjava/).

The following services are currently supported:

* [AWS SDK CloudSearch Grails Plugin](http://github.com/agorapulse/grails-aws-sdk-cloudsearch)
* [AWS SDK DynamoDB Grails Plugin](http://github.com/agorapulse/grails-aws-sdk-dynamodb)
* [AWS SDK Kinesis Grails Plugin](http://github.com/agorapulse/grails-aws-sdk-kinesis)
* [AWS SDK S3 Grails Plugin](http://github.com/agorapulse/grails-aws-sdk-s3)
* [AWS SDK SES Grails Plugin](http://github.com/agorapulse/grails-aws-sdk-ses)
* [AWS SDK SQS Grails Plugin](http://github.com/agorapulse/grails-aws-sdk-sqs)

This plugin encapsulates **Amazon Kinesis** related logic.


# Installation

Add plugin dependency to your `build.gradle`:

```groovy
dependencies {
  ...
  compile 'org.grails.plugins:aws-sdk-kinesis:2.0.0-beta1'
  ...
```

# Config

Create an AWS account [Amazon Web Services](http://aws.amazon.com/), in order to get your own credentials accessKey and secretKey.


## AWS SDK for Java version

You can override the default AWS SDK for Java version by setting it in your _gradle.properties_:

```
awsJavaSdkVersion=1.10.66
```

## Credentials

Add your AWS credentials parameters to your _grails-app/conf/application.yml_:

```yml
grails:
    plugin:
        awssdk:
            accessKey: {ACCESS_KEY}
            secretKey: {SECRET_KEY}
```

If you do not provide credentials, a credentials provider chain will be used that searches for credentials in this order:

* Environment Variables - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_KEY`
* Java System Properties - `aws.accessKeyId and `aws.secretKey`
* Instance profile credentials delivered through the Amazon EC2 metadata service (IAM role)

## Region

The default region used is **us-east-1**. You might override it in your config:

```yml
grails:
    plugin:
        awssdk:
            region: eu-west-1
```

If you're using multiple AWS SDK Grails plugins, you can define specific settings for each services.

```yml
grails:
    plugin:
        awssdk:
            accessKey: {ACCESS_KEY} # Global default setting
            secretKey: {SECRET_KEY} # Global default setting
            region: us-east-1       # Global default setting
            kinesis:
                accessKey: {ACCESS_KEY} # Service setting (optional)
                secretKey: {SECRET_KEY} # Service setting (optional)
                region: eu-west-1       # Service setting (optional)
                stream: MyStream        # Service setting (optional)
                consumerFilterKey: ben  # Service setting (optional)
            
```

**stream** allows you define the stream to use for all requests when using **AwsSdkKinesisService**.
**consumerFilterKey** allows you filter record processing by environment and share a stream between different apps (ex.: devs).

If you use multiple streams, you can create a new service for each stream that inherits from **AmazonKinesisService**.

```groovy
class MyStreamService extends AmazonKinesisService {

    static final STREAM_NAME = 'MyStream'

    @PostConstruct
    def init() {
        init(STREAM_NAME)
    }

}
```


# Usage

The plugin provides the following Grails artefacts:

* **AmazonKinesisService**

And some utility abstract classes to create a stream consumer based on [Kinesis Client Library](http://docs.aws.amazon.com/kinesis/latest/dev/developing-consumers-with-kcl.html):

* **AbstractClientService**
* **AbstractEvent**
* **AbstractEventService**
* **AbstractRecordProcessor**

## Defining stream events

```groovy
import grails.converters.JSON
import grails.plugins.awssdk.kinesis.AbstractEvent
import grails.plugins.awssdk.util.JsonDateUtils
import org.grails.web.json.JSONObject

class MyEvent extends AbstractEvent {

    long accountId
    Date someDate
    String foo
    String bar
    
    // Define the partition logic for Kinesis Stream sharding
    String getPartitionKey() {
        accountId.toString() 
    }
    
    static MyEvent unmarshall(String json) {
        JSONObject jsonObject = JSON.parse(json) as JSONObject
        MyEvent event = new MyEvent(
            accountId: jsonObject.accountId.toLong(),
            consumerFilterKey: jsonObject.consumerFilterKey,
            date: JsonDateUtils.parseJsonDate(jsonObject.date as String),
            partitionKey: jsonObject.partitionKey,
            
        )
        event
    }

}
```

## Recording stream events

```groovy
// Put a record
sequenceNumber = amazonKinesisService.putEvent(new MyEvent(
            accountId: 123456789,
            someDate: new Date(),
            foo: 'foo',
            bar: 'bar'
))
```

## Managing streams

Here are some utility methods to debug streams. But you will probably use a consumer based on [Kinesis Client Library](http://docs.aws.amazon.com/kinesis/latest/dev/developing-consumers-with-kcl.html) to handle events.

```groovy
// List streams
amazonKinesisService.listStreamNames().each {
    println it
}

// Describe stream
stream = amazonKinesisService.describeStream()

// List shards
amazonKinesisService.listShards().each {
    println it
}

// Get a shard
shard = amazonKinesisService.getShard('shardId-000000000002')

// Get oldest record
record = amazonKinesisService.getShardOldestRecord(shard)
println record
println amazonKinesisService.decodeRecordData(record)

// Get record at sequence number
record = amazonKinesisService.getShardRecordAtSequenceNumber(shard, '49547417246700595154645683227660734817370104972359761954')
println record
println amazonKinesisService.decodeRecordData(record)

// List oldest records
amazonKinesisService.listShardRecordsFromHorizon(shard, 100).each { record ->
    println record
    println amazonKinesisService.decodeRecordData(record)
}

// List records after sequence number
amazonKinesisService.listShardRecordsAfterSequenceNumber(shard, '49547417246700595154645683227660734817370104972359761954', 100).each { record ->
    println record
    println amazonKinesisService.decodeRecordData(record)
}
```

If required, you can also directly use **AmazonKinesisClient** instance available at **amazonKinesisService.client**.

For more info, AWS SDK for Java documentation is located here:

* [AWS SDK for Java](http://docs.amazonwebservices.com/AWSJavaSDK/latest/javadoc/index.html)

## Consuming stream events

To create a client based on [Kinesis Client Library](http://docs.aws.amazon.com/kinesis/latest/dev/developing-consumers-with-kcl.html), you must implements 3 classes by extending **AmazonKinesisEventService**, **AmazonKinesisRecordProcessor** and implementing **IRecordProcessorFactory**.

### Event consumer service

Handle and process each event. This is where you put all your business logic.

```groovy
import grails.plugins.awssdk.kinesis.AbstractEventService

class MyStreamEventService extends AbstractEventService {
    
    def handleEvent(MyEvent event) {
        log.debug "Handling event ${event}"
        // Put you consumer business logic here
    }
    
}
```

### Record processor

Unmarshall a single record, create the corresponding event and pass it to the handling consumer service.

```groovy
import com.amazonaws.services.kinesis.model.Record
import grails.plugins.awssdk.kinesis.AbstractRecordProcessor
import groovy.util.logging.Log4j
import org.grails.web.converters.exceptions.ConverterException

@Log4j
class MyStreamRecordProcessor extends AbstractRecordProcessor {

    MyStreamEventService myStreamEventService

    @Override
    void processRecordData(String data, Record record) {
        log.debug "[${shardId}] ${record.sequenceNumber}, ${record.partitionKey}, ${data}"
        MyEvent event = null
        try {
            event = MyEvent.unmarshall(data)
        } catch (ConverterException exception) {
            log.error "Ignoring event: invalid JSON"
        }
        if (event?.accountId) {
            myStreamEventService.handleEvent(event)
        } else {
            log.debug("BAD EVENT ${event}")
        }
    }

}
```

### Record Processor factory

Create the record processor.

```groovy
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory

class MyStreamRecordProcessorFactory implements IRecordProcessorFactory {

    MyStreamEventService myStreamEventService

    @Override
    IRecordProcessor createProcessor() {
        new MyStreamRecordProcessor(myStreamEventService: myStreamEventService)
    }

}
```

### Event consumer client service

Boostrap everything by creating the record processor factory and initializing the client.

If required, during bootstrap, it will automatically create the corresponding Kinesis stream and DynamoDB table for consumer checkpoints.

```groovy
import grails.plugins.awssdk.kinesis.AbstractClientService
import javax.annotation.PostConstruct

class MyStreamClientService extends AbstractClientService {

    static String STREAM_NAME = 'MyStream'
    static long IDLETIME_BETWEEN_READS_MILLIS = 1000L
    
    boolean lazyInit = false
    
    MyStreamEventService myStreamEventService

    @PostConstruct
    def bootstrap() {
        init(
            STREAM_NAME, 
            new MyStreamRecordProcessorFactory(myStreamEventService: myStreamEventService), 
            IDLETIME_BETWEEN_READS_MILLIS
        )
    }

}
```

If required, you can also directly use **AmazonKinesisClient** instance available at **amazonKinesisService.client**.

For more info, AWS SDK for Java documentation is located here:

* [AWS SDK for Java](http://docs.amazonwebservices.com/AWSJavaSDK/latest/javadoc/index.html)


# Bugs

To report any bug, please use the project [Issues](http://github.com/agorapulse/grails-aws-sdk-kinesis/issues) section on GitHub.

Feedback and pull requests are welcome!