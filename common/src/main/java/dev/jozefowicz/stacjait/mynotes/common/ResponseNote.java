package dev.jozefowicz.stacjait.mynotes.common;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.nonNull;

public class ResponseNote {
    private String noteId;
    private String title;
    private String text;
    private long timestamp;
    private NoteType type;
    private Long size;
    private List<String> labels;

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

    public final static ResponseNote fromPersistedNote(PersistedNote persistedNote) {
        ResponseNote note = new ResponseNote();
        note.labels = persistedNote.getLabels();
        note.title = persistedNote.getTitle();
        note.noteId = persistedNote.getNoteId();
        note.text = persistedNote.getText();
        note.timestamp = persistedNote.getTimestamp();
        note.size = persistedNote.getSize();
        note.type = persistedNote.getType();
        return note;
    }

}
