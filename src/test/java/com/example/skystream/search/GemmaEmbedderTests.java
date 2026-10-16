package com.example.skystream.search;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Checks that the Java embedder reproduces the vectors the Python evaluation measured (embedding-fixture.json,
 * recorded with fastembed). Skipped unless the model has been fetched with scripts/fetch-model.sh.
 */
class GemmaEmbedderTests {
    private static final Path MODEL = Path.of(System.getProperty("skystream.search.model-dir", "models/embeddinggemma-300m"));
    private static GemmaEmbedder embedder;
    private static final List<String> documents = new ArrayList<>(), queries = new ArrayList<>();
    private static final List<long[]> documentIds = new ArrayList<>(), queryIds = new ArrayList<>();
    private static final List<float[]> documentVectors = new ArrayList<>(), queryVectors = new ArrayList<>();

    @BeforeAll
    static void load() throws Exception {
        assumeTrue(Files.exists(MODEL.resolve("onnx/model.onnx")), "embedding model not fetched; run scripts/fetch-model.sh");
        embedder = new GemmaEmbedder(MODEL, 2);
        JsonNode fixture = new ObjectMapper().readTree(GemmaEmbedderTests.class.getResourceAsStream("/embedding-fixture.json"));
        for (JsonNode item : fixture.get("items")) {
            boolean query = item.get("role").asString().equals("query");
            (query ? queries : documents).add(item.get("text").asString());
            (query ? queryIds : documentIds).add(longs(item.get("ids")));
            (query ? queryVectors : documentVectors).add(floats(item.get("vector")));
        }
    }

    @AfterAll
    static void close() throws Exception {
        if (embedder != null) embedder.close();
    }

    @Test
    void tokenizesExactlyAsPythonDid() {
        for (int i = 0; i < documents.size(); i++) {
            assertArrayEquals(documentIds.get(i), embedder.tokenize(GemmaEmbedder.DOCUMENT_PREFIX + documents.get(i)),
                    documents.get(i));
        }
        for (int i = 0; i < queries.size(); i++) {
            assertArrayEquals(queryIds.get(i), embedder.tokenize(GemmaEmbedder.QUERY_PREFIX + queries.get(i)), queries.get(i));
        }
    }

    @Test
    void documentVectorsMatchPythonInInputOrder() {
        // Mixed lengths: embedDocuments batches by length internally and must return vectors in input order.
        float[][] vectors = embedder.embedDocuments(documents);
        for (int i = 0; i < documents.size(); i++) {
            assertEquals(1.0, cosine(vectors[i], documentVectors.get(i)), 1e-4, documents.get(i));
        }
    }

    @Test
    void queryVectorsMatchPython() {
        for (int i = 0; i < queries.size(); i++) {
            assertEquals(1.0, cosine(embedder.embedQuery(queries.get(i)), queryVectors.get(i)), 1e-4, queries.get(i));
        }
    }

    @Test
    void vectorsAreNormalized() {
        float[] vector = embedder.embedQuery("washington state hikes");
        assertEquals(1.0, Math.sqrt(IntStream.range(0, vector.length).mapToDouble(i -> vector[i] * vector[i]).sum()), 1e-4);
    }

    private static long[] longs(JsonNode array) {
        long[] values = new long[array.size()];
        for (int i = 0; i < values.length; i++) values[i] = array.get(i).asLong();
        return values;
    }

    private static float[] floats(JsonNode array) {
        float[] values = new float[array.size()];
        for (int i = 0; i < values.length; i++) values[i] = (float) array.get(i).asDouble();
        return values;
    }

    private static double cosine(float[] a, float[] b) {
        double dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        return dot / Math.sqrt(normA * normB);
    }
}
