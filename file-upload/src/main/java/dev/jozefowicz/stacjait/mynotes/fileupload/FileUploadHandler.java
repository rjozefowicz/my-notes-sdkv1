package dev.jozefowicz.stacjait.mynotes.fileupload;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jozefowicz.stacjait.mynotes.common.PersistedNote;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static dev.jozefowicz.stacjait.mynotes.common.APIGatewayProxyResponseEventBuilder.response;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class FileUploadHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final static String BUCKET_NAME = System.getenv("BUCKET_NAME");
    private final static String TABLE_NAME = System.getenv("TABLE_NAME");

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClient.builder().build());
    private final Table myNotesTable = dynamoDB.getTable(TABLE_NAME);

    private final AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
            .build();

    public FileUploadHandler() {
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        try {
            if (isNull(request.getRequestContext().getAuthorizer()) || request.getRequestContext().getAuthorizer().isEmpty()) {
                context.getLogger().log("Authorizer not configured");
                return response(401, null);
            }

            Map<String, String> claims = (Map<String, String>) request.getRequestContext().getAuthorizer().get("claims");
            final String userId = claims.get("cognito:username");

            switch (request.getHttpMethod().toUpperCase()) {
                case "GET":
                    return get(request, userId);
                case "POST":
                    return post(request, userId);
                default:
                    return response(405, null);
            }
        } catch (Exception e) {
            context.getLogger().log("Exception while processing request");
            e.printStackTrace();
            return response(500, null);
        }
    }

    private APIGatewayProxyResponseEvent post(APIGatewayProxyRequestEvent request, String userId) throws IOException {
        if (isNull(request.getBody())) {
            return response(400, null);
        }
        FileUploadRequest fileUploadRestest = this.objectMapper.readValue(request.getBody(), FileUploadRequest.class);
        if (isNull(fileUploadRestest.getName()) || fileUploadRestest.getName().isEmpty()) {
            return response(400, null);
        }
        final String key = userId + "/" + UUID.randomUUID().toString() + "/" + fileUploadRestest.getName();
        return response(200, this.objectMapper.writeValueAsString(SignedUrlResponse.of(presignedUrl(key, HttpMethod.PUT))));
    }

    private APIGatewayProxyResponseEvent get(APIGatewayProxyRequestEvent request, String userId) throws IOException {
        if (nonNull(request.getPathParameters()) && request.getPathParameters().containsKey("id")) {
            GetItemSpec getItemSpec = new GetItemSpec()
                    .withPrimaryKey(new PrimaryKey()
                            .addComponent("userId", userId)
                            .addComponent("noteId", request.getPathParameters().get("id")));
            final Item rawItem = myNotesTable.getItem(getItemSpec);
            final PersistedNote note = this.objectMapper.readValue(rawItem.toJSON(), PersistedNote.class);
            if (!note.getType().isStored()) {
                return response(400, null);
            }
            return response(200, this.objectMapper.writeValueAsString(SignedUrlResponse.of(presignedUrl(note.getS3Location(), HttpMethod.GET))));
        }
        return response(400, null);
    }

    private String presignedUrl(String key, HttpMethod httpMethod) {
        return amazonS3.generatePresignedUrl(new GeneratePresignedUrlRequest(BUCKET_NAME, key, httpMethod)).toString();
    }
}
