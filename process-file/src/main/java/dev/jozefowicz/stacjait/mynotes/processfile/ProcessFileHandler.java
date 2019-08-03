package dev.jozefowicz.stacjait.mynotes.processfile;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClient;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jozefowicz.stacjait.mynotes.common.NoteType;
import dev.jozefowicz.stacjait.mynotes.common.PersistedNote;

import java.net.URLDecoder;
import java.util.List;
import java.util.stream.Collectors;

public class ProcessFileHandler implements RequestHandler<S3EventNotification, Void> {

    private final static String TABLE_NAME = System.getenv("TABLE_NAME");

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClient.builder().build());
    private final Table myNotesTable = dynamoDB.getTable(TABLE_NAME);
    private final AmazonRekognition rekognition = AmazonRekognitionClient.builder().build();

    public ProcessFileHandler() {
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }


    @Override
    public Void handleRequest(S3EventNotification event, Context context) {
        try {
            for (S3EventNotification.S3EventNotificationRecord record : event.getRecords()) {
                final String key = URLDecoder.decode(record.getS3().getObject().getKey(), "UTF-8");
                final String[] idFileName = key.split("/");
                final NoteType type = idFileName[2].matches("(.*/)*.+\\.(png|jpg|gif|bmp|jpeg|PNG|JPG|GIF|BMP)$") ? NoteType.IMAGE : NoteType.FILE;
                final List<String> labels = analyze(key, record.getS3().getBucket().getName());
                final PersistedNote note = PersistedNote.file(idFileName[0], idFileName[1], idFileName[2], key, record.getS3().getObject().getSize(), type, labels);
                persist(note);
            }
            return null;
        } catch (Exception e) {
            context.getLogger().log("Exception while processing S3 event");
            e.printStackTrace();
            return null;
        }
    }

    private void persist(PersistedNote note) throws JsonProcessingException {
        myNotesTable.putItem(Item.fromJSON(this.objectMapper.writeValueAsString(note)));
    }

    private List<String> analyze(String s3Location, String bucketName) {

        final DetectLabelsRequest detectLabelsRequest = new DetectLabelsRequest();
        final S3Object s3Object = new S3Object().withBucket(bucketName).withName(s3Location);
        final Image image = new Image().withS3Object(s3Object);
        detectLabelsRequest
                .withMaxLabels(10)
                .withMinConfidence(75f)
                .withImage(image);
        final DetectLabelsResult detectLabelsResult = rekognition.detectLabels(detectLabelsRequest);
        return detectLabelsResult
                .getLabels()
                .stream()
                .map(Label::getName)
                .collect(Collectors.toList());
    }

}
