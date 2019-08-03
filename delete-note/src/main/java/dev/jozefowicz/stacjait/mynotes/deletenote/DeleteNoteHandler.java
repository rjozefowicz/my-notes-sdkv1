package dev.jozefowicz.stacjait.mynotes.deletenote;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

import static dev.jozefowicz.stacjait.mynotes.common.APIGatewayProxyResponseEventBuilder.response;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class DeleteNoteHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final static String BUCKET_NAME = System.getenv("BUCKET_NAME");
    private final static String TABLE_NAME = System.getenv("TABLE_NAME");

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClient.builder().build());
    private final Table myNotesTable = dynamoDB.getTable(TABLE_NAME);

    private final AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
            .build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {

        try {
            if (!request.getHttpMethod().equalsIgnoreCase("delete")) {
                return response(405, null);
            } else if (isNull(request.getRequestContext().getAuthorizer()) || request.getRequestContext().getAuthorizer().isEmpty()) {
                context.getLogger().log("Authorizer not configured");
                return response(401, null);
            }

            Map<String, String> claims = (Map<String, String>) request.getRequestContext().getAuthorizer().get("claims");
            final String userId = claims.get("cognito:username");

            if (nonNull(request.getPathParameters()) && request.getPathParameters().containsKey("id")) {
                final PrimaryKey primaryKey = new PrimaryKey()
                        .addComponent("userId", userId)
                        .addComponent("noteId", request.getPathParameters().get("id"));
                final DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                        .withPrimaryKey(primaryKey)
                        .withReturnValues(ReturnValue.ALL_OLD);
                final DeleteItemOutcome deleteItemOutcome = myNotesTable.deleteItem(deleteItemSpec);
                final String s3Location = deleteItemOutcome.getItem().getString("s3Location");

                if (nonNull(s3Location)) {
                    amazonS3.deleteObject(new DeleteObjectRequest(BUCKET_NAME, s3Location));
                }

                return response(200, null);
            }

            return response(400, null);
        } catch (Exception e) {
            context.getLogger().log("Exception while processing request");
            e.printStackTrace();
            return response(500, null);
        }
    }


}
