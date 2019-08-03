package dev.jozefowicz.stacjait.mynotes.listnotes;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jozefowicz.stacjait.mynotes.common.PersistedNote;
import dev.jozefowicz.stacjait.mynotes.common.ResponseNote;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static dev.jozefowicz.stacjait.mynotes.common.APIGatewayProxyResponseEventBuilder.response;
import static java.util.Objects.isNull;

public class ListNotesHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private  static final String TABLE_NAME = System.getenv("TABLE_NAME");

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClient.builder().build());
    private final Table myNotesTable = dynamoDB.getTable(TABLE_NAME);

    public ListNotesHandler() {
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {

        try {
            if (!request.getHttpMethod().equalsIgnoreCase("get")) {
                return response(405, null);
            } else if (isNull(request.getRequestContext().getAuthorizer()) || request.getRequestContext().getAuthorizer().isEmpty()) {
                context.getLogger().log("Authorizer not configured");
                return response(401, null);
            }

            /**
             * "claims": {
             *         "sub": "fd3be9de-bd0d-44c9-85e5-bf24a4c5503d",
             *         "aud": "3dohu5vurk9rbc38of88of59k8",
             *         "email_verified": "true",
             *         "event_id": "8deee96b-2010-4d7e-b3d5-d2c4c7af6e99",
             *         "token_use": "id",
             *         "auth_time": "1562705770",
             *         "iss": "https://cognito-idp.eu-west-2.amazonaws.com/eu-west-2_JtSJaeBr1",
             *         "cognito:username": "fd3be9de-bd0d-44c9-85e5-bf24a4c5503d",
             *         "exp": "Mon Jul 15 21:49:52 UTC 2019",
             *         "iat": "Mon Jul 15 20:49:52 UTC 2019",
             *         "email": "radoslawjozefowicz+1@gmail.com"
             *     }
             */

            Map<String, String> claims = (Map<String, String>) request.getRequestContext().getAuthorizer().get("claims");
            final String userId = claims.get("cognito:username");

            final QuerySpec querySpec = new QuerySpec()
                    .withKeyConditionExpression("userId = :userId")
                    .withValueMap(new ValueMap().withString(":userId", userId));
            List<ResponseNote> notes = new ArrayList<>();
            for (Item item : myNotesTable.query(querySpec)) {
                final PersistedNote persistedNote = this.objectMapper.readValue(item.toJSON(), PersistedNote.class);
                ResponseNote note = ResponseNote.fromPersistedNote(persistedNote);
                notes.add(note);
            }
            final Page page = new Page(notes, false);
            return response(200, objectMapper.writeValueAsString(page));
        } catch (Exception e) {
            context.getLogger().log("Exception while processing request");
            e.printStackTrace();
            return response(500, null);
        }
    }

}
