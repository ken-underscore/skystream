package com.example.skystream.search;

import java.util.List;

/** Turns text into normalized vectors for semantic search. Implementations are blocking and CPU-bound. */
public interface Embedder {
    /** Embeds posts (and hypothetical posts), returning one vector per text in input order. */
    float[][] embedDocuments(List<String> texts);

    /** Embeds a search query. */
    float[] embedQuery(String query);
}
