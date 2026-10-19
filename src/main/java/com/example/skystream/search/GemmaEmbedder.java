package com.example.skystream.search;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.stream.IntStream;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;

/**
 * embeddinggemma-300m through ONNX Runtime, reproducing fastembed's preprocessing (and so the Python evaluation's
 * vectors). The model's sentence_embedding output is already normalized.
 */
public class GemmaEmbedder implements Embedder, AutoCloseable {
    static final String QUERY_PREFIX = "task: search result | query: ";
    static final String DOCUMENT_PREFIX = "title: none | text: ";
    private static final long PAD_ID = 0;   // <pad>
    private static final int MAX_TOKENS = 2048;
    private static final int BATCH = 32;

    private final OrtEnvironment environment = OrtEnvironment.getEnvironment();
    private final HuggingFaceTokenizer tokenizer;
    private final OrtSession session;

    public GemmaEmbedder(Path modelDir, int threads) {
        try {
            // tokenizer.json's template adds <bos> and <eos>; batches are padded here to their longest text.
            // tokenizer_config.json carries the model's 2048-token limit: without it, DJL caps texts at 512.
            tokenizer = HuggingFaceTokenizer.builder()
                    .optTokenizerPath(modelDir.resolve("tokenizer.json"))
                    .optTokenizerConfigPath(modelDir.resolve("tokenizer_config.json").toString())
                    .optAddSpecialTokens(true)
                    .optTruncation(true)
                    .optMaxLength(MAX_TOKENS)
                    .optPadding(false)
                    .build();
            try (var options = new OrtSession.SessionOptions()) {
                options.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);
                options.setIntraOpNumThreads(threads);
                // model.onnx_data must be a real file (or hard link) next to model.onnx: ONNX Runtime rejects
                // symlinks that resolve outside the model directory.
                session = environment.createSession(modelDir.resolve("onnx/model.onnx").toString(), options);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not load the embedding model from " + modelDir, e);
        }
    }

    /**
     * Batches texts of similar length together, which cuts padding work, and returns vectors in input order. Stops
     * between batches if the thread is interrupted.
     */
    @Override
    public float[][] embedDocuments(List<String> texts) {
        int[] byLength = IntStream.range(0, texts.size()).boxed()
                .sorted(Comparator.comparingInt(i -> texts.get(i).length())).mapToInt(Integer::intValue).toArray();
        var vectors = new float[texts.size()][];
        for (int start = 0; start < byLength.length; start += BATCH) {
            // A run can't be interrupted, so an interrupt is honored between runs instead.
            if (Thread.currentThread().isInterrupted()) throw new CancellationException("Interrupted while embedding");
            int[] batch = Arrays.copyOfRange(byLength, start, Math.min(start + BATCH, byLength.length));
            float[][] embedded = run(IntStream.of(batch).mapToObj(i -> DOCUMENT_PREFIX + texts.get(i)).toList());
            for (int i = 0; i < batch.length; i++) vectors[batch[i]] = embedded[i];
        }
        return vectors;
    }

    @Override
    public float[] embedQuery(String query) {
        return run(List.of(QUERY_PREFIX + query))[0];
    }

    long[] tokenize(String prefixedText) {
        return tokenizer.encode(prefixedText).getIds();
    }

    private float[][] run(List<String> prefixedTexts) {
        long[][] encoded = prefixedTexts.stream().map(this::tokenize).toArray(long[][]::new);
        int length = 0;
        for (long[] ids : encoded) length = Math.max(length, ids.length);
        long[][] ids = new long[encoded.length][length];
        long[][] mask = new long[encoded.length][length];
        for (int row = 0; row < encoded.length; row++) {
            for (int column = 0; column < length; column++) {
                boolean token = column < encoded[row].length;
                ids[row][column] = token ? encoded[row][column] : PAD_ID;
                mask[row][column] = token ? 1 : 0;
            }
        }
        try (var idTensor = OnnxTensor.createTensor(environment, ids);
             var maskTensor = OnnxTensor.createTensor(environment, mask);
             var result = session.run(Map.of("input_ids", idTensor, "attention_mask", maskTensor),
                     Set.of("sentence_embedding"))) {
            return (float[][]) result.get("sentence_embedding").orElseThrow().getValue();
        } catch (OrtException e) {
            throw new IllegalStateException("Embedding failed", e);
        }
    }

    @Override
    public void close() throws OrtException {
        try {
            session.close();
        } finally {
            tokenizer.close();
        }
    }
}
