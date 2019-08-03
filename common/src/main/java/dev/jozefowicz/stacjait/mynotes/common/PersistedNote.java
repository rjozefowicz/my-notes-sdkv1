package dev.jozefowicz.stacjait.mynotes.common;

import java.util.List;
import java.util.UUID;

public class PersistedNote {
    private String userId;
    private String noteId;
    private String title;
    private String text;
    private long timestamp;
    private NoteType type;
    private Long size;
    private List<String> labels;
    private String s3Location;

    public String getNoteId() {
        return noteId;
    }

    public String getTitle() {
        return title;
    }

    public String getText() {
        return text;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public NoteType getType() {
        return type;
    }

    public Long getSize() {
        return size;
    }

    public List<String> getLabels() {
        return labels;
    }

    public String getUserId() {
        return userId;
    }

    public String getS3Location() {
        return s3Location;
    }

    public static final PersistedNote updated(String userId, String noteId, String title, String text, List<String> labels) {
        PersistedNote note = new PersistedNote();
        note.userId = userId;
        note.noteId = noteId;
        note.labels = labels;
        note.type = NoteType.TEXT;
        note.title = title;
        note.text = text;
        note.timestamp = System.currentTimeMillis();
        return note;
    }

    public static final PersistedNote create(String userId, String title, String text, List<String> labels) {
        PersistedNote note = new PersistedNote();
        note.userId = userId;
        note.noteId = UUID.randomUUID().toString();
        note.labels = labels;
        note.type = NoteType.TEXT;
        note.title = title;
        note.text = text;
        note.timestamp = System.currentTimeMillis();
        return note;
    }

    public static final PersistedNote file(String userId, String noteId, String title, String s3Location, long size, NoteType type, List<String> labels) {
        PersistedNote note = new PersistedNote();
        note.userId = userId;
        note.noteId = noteId;
        note.labels = labels;
        note.type = type;
        note.title = title;
        note.size = size;
        note.s3Location = s3Location;
        note.timestamp = System.currentTimeMillis();
        return note;
    }

}
