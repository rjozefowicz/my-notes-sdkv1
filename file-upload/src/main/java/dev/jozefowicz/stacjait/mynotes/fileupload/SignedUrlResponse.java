package dev.jozefowicz.stacjait.mynotes.fileupload;

public class SignedUrlResponse {
    private final String link;

    private SignedUrlResponse(String link) {
        this.link = link;
    }

    public static final SignedUrlResponse of(String link) {
        return new SignedUrlResponse(link);
    }
}
