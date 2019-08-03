package dev.jozefowicz.stacjait.mynotes.listnotes;

import java.util.List;

public class Page<T> {
    private final List<T> elements;
    private final boolean hasNext;

    public Page(List<T> elements, boolean hasNext) {
        this.elements = elements;
        this.hasNext = hasNext;
    }

    public List<T> getElements() {
        return elements;
    }

    public boolean isHasNext() {
        return hasNext;
    }

}
