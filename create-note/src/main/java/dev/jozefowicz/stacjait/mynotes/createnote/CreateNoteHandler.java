package dev.jozefowicz.stacjait.mynotes.createnote;

import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.AmazonComprehendClient;
import com.amazonaws.services.comprehend.model.DetectDominantLanguageRequest;
import com.amazonaws.services.comprehend.model.DetectDominantLanguageResult;
import com.amazonaws.services.comprehend.model.DetectEntitiesRequest;
import com.amazonaws.services.comprehend.model.DominantLanguage;
import com.amazonaws.services.comprehend.model.Entity;
import com.amazonaws.services.comprehend.model.LanguageCode;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jozefowicz.stacjait.mynotes.common.PersistedNote;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static dev.jozefowicz.stacjait.mynotes.common.APIGatewayProxyResponseEventBuilder.response;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class CreateNoteHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final static String TABLE_NAME = System.getenv("TABLE_NAME");
    private final static List<String> SUPPORTED_LANGUAGES = Arrays.asList(LanguageCode.values()).stream().map(code -> code.toString()).collect(Collectors.toList());

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClient.builder().build());
    private final Table myNotesTable = dynamoDB.getTable(TABLE_NAME);

    private final AmazonComprehend comprehendClient = AmazonComprehendClient.builder().build();

    public CreateNoteHandler() {
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

            if (isNull(request.getBody())) {
                return response(400, null);
            }

            final PersistedNote note = this.objectMapper.readValue(request.getBody(), PersistedNote.class);

            if (invalid(note)) {
                return response(400, null);
            }

            switch (request.getHttpMethod().toUpperCase()) {
                case "PUT":
                    if (nonNull(request.getPathParameters()) && request.getPathParameters().containsKey("id")) {
                        return put(userId, request.getPathParameters().get("id"), note);
                    }
                    return response(400, null);
                case "POST":
                    return post(userId, note);
                default:
                    return response(405, null);
            }
        } catch (Exception e) {
            context.getLogger().log("Exception while processing request");
            e.printStackTrace();
            return response(500, null);
        }
    }

    private APIGatewayProxyResponseEvent post(String userId, PersistedNote note) throws JsonProcessingException {
        PersistedNote newNote = PersistedNote.create(userId, note.getTitle(), note.getText(), analyze(note.getText()));
        persist(newNote);
        return response(200, null);
    }

    private boolean invalid(PersistedNote note) {
        return isNull(note.getText()) || note.getText().isEmpty() || isNull(note.getTitle()) || note.getTitle().isEmpty();
    }

    private APIGatewayProxyResponseEvent put(String userId, String noteId, PersistedNote note) throws JsonProcessingException {
        PersistedNote updated = PersistedNote.updated(userId, noteId, note.getTitle(), note.getText(), analyze(note.getText()));
        persist(updated);
        return response(200, null);
    }

    private List<String> analyze(String textToAnalyze) {
        // detecting dominant languages
        DetectDominantLanguageResult dominantLanguage = comprehendClient.detectDominantLanguage(new DetectDominantLanguageRequest().withText(textToAnalyze));
        dominantLanguage.getLanguages().stream().map(l -> l.getLanguageCode()).forEach(System.out::println);
        return new ArrayList<>(dominantLanguage.getLanguages()
                .stream()
                .map(DominantLanguage::getLanguageCode) // mapping to detected language codes
                .filter(SUPPORTED_LANGUAGES::contains) // filtering to supported ones
                .map(code -> comprehendClient.detectEntities(new DetectEntitiesRequest().withText(textToAnalyze).withLanguageCode(code))) // analyzing text
                .flatMap(detectEntitiesResponse -> detectEntitiesResponse.getEntities().stream())// flatmapping to get list of entities
                .map(Entity::getText)// getting labels
                .collect(Collectors.toSet()));
    }

    private void persist(PersistedNote note) throws JsonProcessingException {
        myNotesTable.putItem(Item.fromJSON(this.objectMapper.writeValueAsString(note)));
    }

}
