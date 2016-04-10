package grails.plugin.awssdk.kinesis

import com.amazonaws.ClientConfiguration
import com.amazonaws.regions.Region
import com.amazonaws.regions.ServiceAbbreviations
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.*
import grails.core.GrailsApplication
import grails.plugin.awssdk.AwsClientUtil
import org.springframework.beans.factory.InitializingBean

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.Charset
import java.nio.charset.CharsetDecoder
import java.nio.charset.CharsetEncoder

class AmazonKinesisService implements InitializingBean {

    static SERVICE_NAME = ServiceAbbreviations.Kinesis

    static String DEFAULT_SHARD_ITERATOR_TYPE = 'LATEST'
    static long DEFAULT_GET_RECORDS_WAIT = 1000 // wait for 1s before getting next batch
    static int DEFAULT_LIST_RECORDS_LIMIT = 1000 // 1000 records list limit
    static int MAX_PUT_RECORDS_SIZE = 500

    CharsetEncoder encoder = Charset.forName('UTF-8').newEncoder()
    CharsetDecoder decoder = Charset.forName('UTF-8').newDecoder()

    GrailsApplication grailsApplication
    AmazonKinesisClient client
    private String defaultStreamName = ''

    void afterPropertiesSet() throws Exception {
        // Set region
        Region region = AwsClientUtil.buildRegion(config, serviceConfig)
        assert region?.isServiceSupported(SERVICE_NAME)

        // Create client
        def credentials = AwsClientUtil.buildCredentials(config, serviceConfig)
        ClientConfiguration configuration = AwsClientUtil.buildClientConfiguration(config, serviceConfig)
        client = new AmazonKinesisClient(credentials, configuration)
                .withRegion(region)

        defaultStreamName = serviceConfig?.stream ?: ''
    }

    protected void init(String streamName) {
        defaultStreamName = streamName
    }

    /**
     *
     * @param shardCount
     */
    void createStream(int shardCount = 1) {
        assertDefaultStreamName()
        createStream(defaultStreamName, shardCount)
    }

    /**
     *
     * @param record
     * @return
     */
    String decodeRecordData(Record record) {
        String data = ''
        try {
            // For this app, we interpret the payload as UTF-8 chars.
            data = decoder.decode(record.data).toString()
        } catch (CharacterCodingException e) {
            log.error "Malformed data for record: ${record}", e
        }
        data
    }

    /**
     *
     * @return
     */
    DescribeStreamResult describeStream() {
        assertDefaultStreamName()
        describeStream(defaultStreamName)
    }

    /**
     *
     * @param shardId
     * @return
     */
    Shard getShard(String shardId) {
        getShard(defaultStreamName, shardId)
    }

    /**
     *
     * @param shard
     * @return
     */
    Record getShardOldestRecord(Shard shard) {
        assertDefaultStreamName()
        List records = listShardRecords(defaultStreamName, shard, 'TRIM_HORIZON', '', 1)
        if (records) {
            records.first()
        }
    }

    /**
     *
     * @param shard
     * @param sequenceNumber
     * @return
     */
    Record getShardRecordAtSequenceNumber(Shard shard,
                                          String sequenceNumber) {
        assertDefaultStreamName()
        List records = listShardRecords(defaultStreamName, shard, 'AT_SEQUENCE_NUMBER', sequenceNumber, 1)
        if (records) {
            records.first()
        }
    }

    /**
     *
     * @return
     */
    List<String> listStreamNames() {
        client.listStreams().streamNames
    }

    /**
     *
     * @param shard
     * @param startingSequenceNumber
     * @param limit
     * @param batchLimit
     * @return
     */
    List<Record> listShardRecordsAfterSequenceNumber(Shard shard,
                                                     String startingSequenceNumber,
                                                     int limit = DEFAULT_LIST_RECORDS_LIMIT,
                                                     int batchLimit = 0) {
        assertDefaultStreamName()
        listShardRecords(defaultStreamName, shard, 'AFTER_SEQUENCE_NUMBER', startingSequenceNumber, limit, batchLimit)
    }

    /**
     *
     * @param shard
     * @param limit
     * @param batchLimit
     * @return
     */
    List<Record> listShardRecordsFromHorizon(Shard shard,
                                             int limit = DEFAULT_LIST_RECORDS_LIMIT,
                                             int batchLimit = 0) {
        assertDefaultStreamName()
        listShardRecords(defaultStreamName, shard, 'TRIM_HORIZON', '', limit, batchLimit)
    }


    /**
     *
     * @return
     */
    List<Shard> listShards() {
        assertDefaultStreamName()
        listShards(defaultStreamName)
    }

    /**
     *
     * @param event
     * @return
     */
    String putEvent(AbstractEvent event) {
        if (serviceConfig?.consumerFilterKey) {
            event.consumerFilterKey = serviceConfig?.consumerFilterKey
        }
        putRecord(event.partitionKey, event.encodeAsJSON().toString())
    }

    /**
     *
     * @param events
     * @return
     */
    String putEvents(List<AbstractEvent> events) {
        assert events.size() < MAX_PUT_RECORDS_SIZE, "Max put events size is ${MAX_PUT_RECORDS_SIZE}"
        List<PutRecordsRequestEntry> records = []
        events.each { AbstractEvent event ->
            if (serviceConfig?.consumerFilterKey) {
                event.consumerFilterKey = serviceConfig.consumerFilterKey
            }
            records << new PutRecordsRequestEntry(
                    data: ByteBuffer.wrap(event.encodeAsJSON().toString().bytes),
                    partitionKey: event.partitionKey
            )
        }
        putRecords(records)
    }

    /**
     *
     * @param partitionKey
     * @param data
     * @param sequenceNumberForOrdering
     * @return
     */
    String putRecord(String partitionKey,
                     String data,
                     String sequenceNumberForOrdering = '') {
        assertDefaultStreamName()
        putRecord(defaultStreamName, partitionKey, data, sequenceNumberForOrdering)
    }

    /**
     *
     * @param records
     * @return
     */
    PutRecordsResult putRecords(List<PutRecordsRequestEntry> records) {
        assertDefaultStreamName()
        putRecords(defaultStreamName, records)
    }

    /**
     *
     * @param streamName
     * @param records
     * @return
     */
    PutRecordsResult putRecords(String streamName,
                                List<PutRecordsRequestEntry> records) {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest(
                records: records,
                streamName: streamName
        )

        client.putRecords(putRecordsRequest)
    }

    /**
     *
     * @param shardId1
     * @param shardId2
     */
    void mergeShards(String shardId1,
                     String shardId2) {
        mergeShards(defaultStreamName, shardId1, shardId2)
    }

    /**
     *
     * @param shardId
     */
    void splitShard(String shardId) {
        splitShard(defaultStreamName, shardId)
    }

    // PRIVATE

    protected void createStream(String streamName,
                                int shardCount = 1) {
        client.createStream(streamName, shardCount)
    }

    protected DescribeStreamResult describeStream(String streamName) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest(
                streamName: streamName
        )
        DescribeStreamResult describeStreamResult

        String exclusiveStartShardId
        List<Shard> shards = []
        while(true) {
            describeStreamRequest.exclusiveStartShardId = exclusiveStartShardId
            describeStreamResult = client.describeStream(describeStreamRequest)
            shards.addAll(describeStreamResult.streamDescription.shards)
            if (shards && describeStreamResult.streamDescription.hasMoreShards) {
                exclusiveStartShardId = shards[-1].shardId
            } else {
                break
            }
        }
        describeStreamResult.streamDescription.shards = shards
        describeStreamResult
    }

    protected Shard getShard(String streamName,
                             String shardId) {
        Shard shard
        DescribeStreamResult describeStreamResult = describeStream(streamName)
        if (describeStreamResult.streamDescription.shards) {
            shard = describeStreamResult.streamDescription.shards.find { Shard it ->
                it.shardId == shardId
            }
        }
        shard
    }

    protected List<Record> listShardRecords(String streamName,
                                            Shard shard,
                                            String shardIteratorType,
                                            String startingSequenceNumber,
                                            int limit,
                                            int batchLimit = 0,
                                            int maxLoopCount = 10) {
        List records = []
        // Get shard iterator
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest(
                streamName: streamName,
                shardId: shard.shardId,
                shardIteratorType: shardIteratorType,
        )
        if (shardIteratorType in ['AFTER_SEQUENCE_NUMBER', 'AT_SEQUENCE_NUMBER']) {
            assert startingSequenceNumber
            getShardIteratorRequest.startingSequenceNumber = startingSequenceNumber
        }

        GetShardIteratorResult getShardIteratorResult = client.getShardIterator(getShardIteratorRequest)
        String shardIterator = getShardIteratorResult.shardIterator

        // Get records
        int loopCount = 0
        while (loopCount < maxLoopCount) {
            GetRecordsRequest getRecordsRequest = new GetRecordsRequest(shardIterator: shardIterator)
            if (batchLimit) {
                getRecordsRequest.limit = batchLimit
            }

            GetRecordsResult getRecordsResult = client.getRecords(getRecordsRequest)

            if (getRecordsResult.records) {
                records.addAll(getRecordsResult.records)
            }

            if (records.size() >= limit) {
                // Limit reached
                break
            }


            try {
                Thread.sleep(DEFAULT_GET_RECORDS_WAIT)
            } catch (InterruptedException exception) {
                throw new RuntimeException(exception)
            }

            shardIterator = getRecordsResult.nextShardIterator
            loopCount++
        }
        records.take(limit)
    }

    protected List<Shard> listShards(String streamName) {
        describeStream(streamName)?.streamDescription?.shards
    }

    protected void mergeShards(String streamName,
                               String shardId1,
                               String shardId2) {
        MergeShardsRequest mergeShardsRequest = new MergeShardsRequest(
                streamName: streamName,
                shardToMerge: shardId1,
                adjacentShardToMerge: shardId2
        )

        client.mergeShards(mergeShardsRequest)
    }

    protected String putRecord(String streamName,
                               String partitionKey,
                               String data,
                               String sequenceNumberForOrdering) {
        PutRecordRequest putRecordRequest = new PutRecordRequest(
                data: ByteBuffer.wrap(data.bytes),
                partitionKey: partitionKey,
                streamName: streamName
        )
        if (sequenceNumberForOrdering) {
            putRecordRequest.sequenceNumberForOrdering = sequenceNumberForOrdering
        }

        PutRecordResult putRecordResult = client.putRecord(putRecordRequest)
        putRecordResult.sequenceNumber
    }

    protected void splitShard(String streamName,
                              String shardId) {
        Shard shard = getShard(shardId)
        assert shard, "Shard not found for shardId=$shardId"

        splitShard(streamName, shard)
    }

    protected void splitShard(String streamName,
                              Shard shard,
                              String newStartingHashKey = '') {
        if (!newStartingHashKey) {
            // Determine the hash key value that is half-way between the lowest and highest values in the shard
            BigInteger startingHashKey = new BigInteger(shard.hashKeyRange.startingHashKey)
            BigInteger endingHashKey = new BigInteger(shard.hashKeyRange.endingHashKey)
            newStartingHashKey = startingHashKey.add(endingHashKey).divide(new BigInteger("2")).toString()
        }

        SplitShardRequest splitShardRequest = new SplitShardRequest(
                streamName: streamName,
                shardToSplit: shard.shardId,
                newStartingHashKey: newStartingHashKey
        )

        client.splitShard(splitShardRequest)
    }

    // PRIVATE

    boolean assertDefaultStreamName() {
        assert defaultStreamName, "Default stream must be defined"
    }

    def getConfig() {
        grailsApplication.config.grails?.plugin?.awssdk ?: grailsApplication.config.grails?.plugins?.awssdk
    }

    def getServiceConfig() {
        config[SERVICE_NAME]
    }

}
