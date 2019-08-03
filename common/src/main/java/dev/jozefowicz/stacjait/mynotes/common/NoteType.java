package dev.jozefowicz.stacjait.mynotes.common;

public enum NoteType {
    TEXT, IMAGE, FILE;

    public boolean isStored() {
        return this == FILE || this == IMAGE;
    }
}
