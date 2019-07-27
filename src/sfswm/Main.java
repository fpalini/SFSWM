package sfswm;

import fastdoop.FASTAshortInputFileFormat;
import fastdoop.Record;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Main {

    private static final byte[] dnaIdMap = new byte[255];
    private static final byte[] dnaIdMapRev = new byte[255];

    private static final int[][][] Chiaromonte = new int[4][][];
    private static final byte[][][] mismatches = new byte[4][][];

    static {
        // dnaIdMap['A'] = 0; // default
        dnaIdMap['C'] = 1;
        dnaIdMap['G'] = 2;
        dnaIdMap['T'] = 3;

        dnaIdMapRev['A'] = 3;
        dnaIdMapRev['C'] = 2;
        dnaIdMapRev['G'] = 1;
        // dnaIdMapRev['T'] = 0; // default

        int[][] Chiaromonte1 = {{   91, -114,  -31, -123 },
                { -114,  100, -125,  -31 },
                {  -31, -125,  100, -114 },
                { -123,  -31, -114,   91 }};

        byte[][] mismatches1 = new byte[4][4]; // 16B

        int[][] Chiaromonte2 = new int[16][16];  // 1KB
        byte[][] mismatches2 = new byte[16][16]; // 256B

        int[][] Chiaromonte3 = new int[64][64];  // 16KB
        byte[][] mismatches3 = new byte[64][64]; // 4KB

        int[][] Chiaromonte4 = new int[256][256];  // 256KB
        byte[][] mismatches4 = new byte[256][256]; // 64KB

        byte[] nucleotides = {0, 1, 2, 3};
        int pair1, pair2, triplet1, triplet2, quartet1, quartet2;

        for (byte n1 : nucleotides)
            for (byte n2 : nucleotides) {
                mismatches1[n1][n2] = (byte) (n1 != n2? 1 : 0);

                for (byte n3 : nucleotides)
                    for (byte n4 : nucleotides) {

                        pair1 = pair2byte(n1, n3) & 0xFF;
                        pair2 = pair2byte(n2, n4) & 0xFF;

                        Chiaromonte2[pair1][pair2] = Chiaromonte1[n1][n2] + Chiaromonte1[n3][n4];
                        mismatches2[pair1][pair2] = (byte) (mismatches1[n1][n2] + (n3 != n4? 1 : 0));

                        for (byte n5 : nucleotides)
                            for (byte n6 : nucleotides) {

                                triplet1 = triplet2byte(n1, n3, n5) & 0xFF;
                                triplet2 = triplet2byte(n2, n4, n6) & 0xFF;

                                Chiaromonte3[triplet1][triplet2] = Chiaromonte2[pair1][pair2] + Chiaromonte1[n5][n6];
                                mismatches3[triplet1][triplet2] = (byte) (mismatches2[pair1][pair2] + (n5 != n6? 1 : 0));

                                for (byte n7 : nucleotides)
                                    for (byte n8 : nucleotides) {

                                        quartet1 = quartet2byte(n1, n3, n5, n7) & 0xFF;
                                        quartet2 = quartet2byte(n2, n4, n6, n8) & 0xFF;

                                        Chiaromonte4[quartet1][quartet2] = Chiaromonte3[triplet1][triplet2] + Chiaromonte1[n7][n8];
                                        mismatches4[quartet1][quartet2] = (byte) (mismatches3[triplet1][triplet2] + (n7 != n8? 1 : 0));
                                    }
                            }
                    }
            }

        Chiaromonte[0] = Chiaromonte4;
        Chiaromonte[1] = Chiaromonte1;
        Chiaromonte[2] = Chiaromonte2;
        Chiaromonte[3] = Chiaromonte3;

        mismatches[0] = mismatches4;
        mismatches[1] = mismatches1;
        mismatches[2] = mismatches2;
        mismatches[3] = mismatches3;
    }

    private static byte pair2byte(byte n1, byte n2) {
        byte b = (byte) (n1 << 2);
        b |= n2;

        return b;
    }

    private static byte triplet2byte(byte n1, byte n2, byte n3) {
        byte b = (byte) (n1 << 4);
        b |= n2 << 2;
        b |= n3;

        return b;
    }

    private static byte quartet2byte(byte n1, byte n2, byte n3, byte n4) {
        byte b = (byte) (n1 << 6);
        b |= n2 << 4;
        b |= n3 << 2;
        b |= n4;

        return b;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("yarn").setAppName("SFSWM");
        JavaSparkContext spark = new JavaSparkContext(conf);

        String input_path = args[0];
        int num_slices = Integer.parseInt(args[1]);
        int fastdoop_buffer_size = Integer.parseInt(args[2]);

        Broadcast<Integer> chunk_len = spark.broadcast(Integer.parseInt(args[3]));

        spark.hadoopConfiguration().setInt("look_ahead_buffer_size", fastdoop_buffer_size);

        JavaRDD<Record> records = spark.newAPIHadoopFile(input_path, FASTAshortInputFileFormat.class, Text.class, Record.class, spark.hadoopConfiguration()).values();

        // <id, record> --> <id, key>
        Broadcast<List<String>> id2key_bc = spark.broadcast(records.map(Record::getKey).collect());

        // <record> --> <id, record>
        JavaPairRDD<Integer, Record> id_record_rdd = records.zipWithIndex().mapToPair(record_id -> new Tuple2<>(record_id._2.intValue(), record_id._1));

        Broadcast<int[]> onesPosition, dontcaresPosition;
        Broadcast<Integer> l, w, n_dontcares;
        Broadcast<Integer> n_dontcares_div, n_dontcares_mod;
        Broadcast<Integer> chunk_offset;
        Broadcast<Integer> n;

        Broadcast<Integer> threshold = spark.broadcast(0);
        String pattern = "100000101000000000000101100000000000000000010000000010000100000010001001";

        byte[] pattern_array = pattern.getBytes();

        ArrayList<Integer> onesPosition_list = new ArrayList<>();
        ArrayList<Integer> dontcaresPosition_list = new ArrayList<>();

        for (int i = 0; i < pattern_array.length; i++)
            if (pattern_array[i] == '1')
                onesPosition_list.add(i);
            else
                dontcaresPosition_list.add(i);

        onesPosition = spark.broadcast(onesPosition_list.stream().mapToInt(i -> i).toArray());
        dontcaresPosition = spark.broadcast(dontcaresPosition_list.stream().mapToInt(i -> i).toArray());

        l = spark.broadcast(pattern_array.length);
        w = spark.broadcast(onesPosition_list.size());

        n_dontcares = spark.broadcast(l.getValue()-w.getValue());
        chunk_offset = spark.broadcast(chunk_len.getValue() - (l.getValue()-1));
        n_dontcares_div = spark.broadcast((int) Math.ceil(n_dontcares.getValue() / 4.0));
        n_dontcares_mod = spark.broadcast(n_dontcares.getValue() % 4);
        n = spark.broadcast(id2key_bc.getValue().size());


        // <id, record>  -->  <chunk, (id_offset, len)>
        JavaPairRDD<byte[], Tuple2<Long, Integer>> chunk_id_off_rdd = id_record_rdd.flatMapToPair(record_id -> {
            List<Tuple2<byte[], Tuple2<Long, Integer>>> id_chunk_list = new ArrayList<>();

            byte[] seq = record_id._2.getBuffer();
            int start = record_id._2.getStartValue();
            int end = record_id._2.getEndValue();
            int seqLen = end - start + 1;

            long id_offset;

            for (int i = 0; i < seqLen - (l.getValue()-1); i += chunk_offset.getValue()) {
                id_offset = record_id._1;
                id_offset <<= 32;
                id_offset += i;

                id_chunk_list.add(new Tuple2<>(Arrays.copyOfRange(seq, start+i, Math.min(start+i+chunk_len.getValue(), end)), new Tuple2<>(id_offset, seqLen)));
            }

            return id_chunk_list.iterator();
        }).repartition(num_slices);


        // <chunk, (id_offset, len)>  -->  <W, (id_pos, dontcares)>
        JavaPairRDD<Integer, Tuple2<Long, byte[]>> spacedWords_rdd = chunk_id_off_rdd.flatMapToPair(chunk_id_off_len -> {

            int len = chunk_id_off_len._1.length;
            int W;
            int bits;
            int reset_bits = 2*w.getValue()- 2;
            byte [] dontcares;

            byte[] chunk = new byte[chunk_id_off_len._1.length];

            ArrayList<Tuple2<Integer, Tuple2<Long, byte[]>>> spacedWords = new ArrayList<>(len - (l.getValue() - 1));

            for (int i = 0; i < chunk_id_off_len._1.length; i++)
                chunk[i] = dnaIdMap[chunk_id_off_len._1[i]];

            for (int pos = 0; pos < len - (l.getValue()-1); pos++) {
                W = 0;
                bits = reset_bits;

                for (int i = 0; i < w.getValue(); i++) {
                    W |= chunk[pos + onesPosition.getValue()[i]] << bits;
                    bits -= 2;
                }

                dontcares = new byte[n_dontcares_div.getValue()];
                int i;

                for (i = 0; i < n_dontcares.getValue() - n_dontcares_mod.getValue(); i+=4)
                    dontcares[i/4] = quartet2byte(chunk[pos + dontcaresPosition.getValue()[i]], chunk[pos + dontcaresPosition.getValue()[i+1]], chunk[pos + dontcaresPosition.getValue()[i+2]], chunk[pos + dontcaresPosition.getValue()[i+3]]);

                switch(n_dontcares_mod.getValue()) {
                    case 0: break;
                    case 1: dontcares[i/4] = chunk[pos + dontcaresPosition.getValue()[i]]; break;
                    case 2: dontcares[i/4] = pair2byte(chunk[pos + dontcaresPosition.getValue()[i]], chunk[pos + dontcaresPosition.getValue()[i+1]]); break;
                    case 3: dontcares[i/4] = triplet2byte(chunk[pos + dontcaresPosition.getValue()[i]], chunk[pos + dontcaresPosition.getValue()[i+1]], chunk[pos + dontcaresPosition.getValue()[i+2]]); break;
                    default: throw new Exception("Wrong number of bytes exceeded: "+n_dontcares_mod.getValue());
                }

                spacedWords.add(new Tuple2<>(W, new Tuple2<>(chunk_id_off_len._2._1 + pos, dontcares)));
            }

            // Complementary-Reverse

            int seqLen = chunk_id_off_len._2._2;
            int offset = chunk_id_off_len._2._1.intValue();

            long id = (chunk_id_off_len._2._1 >> 32) + n.getValue();
            id <<= 32;

            for (int i = 0; i < chunk_id_off_len._1.length; i++)
                chunk[i] = dnaIdMapRev[chunk_id_off_len._1[i]];

            for (int pos = l.getValue()-1; pos < len; pos++) {
                W = 0;
                bits = reset_bits;

                for (int i = 0; i < w.getValue(); i++) {
                    W |= chunk[pos - onesPosition.getValue()[i]] << bits;
                    bits -= 2;
                }

                dontcares = new byte[n_dontcares_div.getValue()];
                int i;

                for (i = 0; i < n_dontcares.getValue() - n_dontcares_mod.getValue(); i+=4)
                    dontcares[i/4] = quartet2byte(chunk[pos - dontcaresPosition.getValue()[i]], chunk[pos - dontcaresPosition.getValue()[i+1]], chunk[pos - dontcaresPosition.getValue()[i+2]], chunk[pos - dontcaresPosition.getValue()[i+3]]);

                switch(n_dontcares_mod.getValue()) {
                    case 0: break;
                    case 1: dontcares[i/4] = chunk[pos - dontcaresPosition.getValue()[i]]; break;
                    case 2: dontcares[i/4] = pair2byte(chunk[pos - dontcaresPosition.getValue()[i]], chunk[pos - dontcaresPosition.getValue()[i+1]]); break;
                    case 3: dontcares[i/4] = triplet2byte(chunk[pos - dontcaresPosition.getValue()[i]], chunk[pos - dontcaresPosition.getValue()[i+1]], chunk[pos - dontcaresPosition.getValue()[i+2]]); break;
                    default: throw new Exception("Wrong number of bytes exceeded: "+n_dontcares_mod.getValue());
                }

                spacedWords.add(new Tuple2<>(W, new Tuple2<>(id + (seqLen-1) - (offset + pos), dontcares)));
            }

            return spacedWords.iterator();
        });


        // <W, [(id, pos, dontcares)], [(id, posRev, dontcaresRev)]>  -->  <(id1, id2), nMismatch>
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> mismatches_rdd = spacedWords_rdd.groupByKey().flatMapToPair(W_list -> {
            ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>> mismatches_list = new ArrayList<>();

            Iterator<Tuple2<Long, byte[]>> iter1, iter2;
            Tuple2<Long, byte[]> id_pos_dcares1, id_pos_dcares2;
            int n_v = n.getValue();
            int id1, id2;
            int pos1, pos2;

            int score, nMismatches, i;
            boolean threshBroken;

            Object2IntOpenHashMap<Match> matches = new Object2IntOpenHashMap<>();

            iter1 = W_list._2.iterator();
            int n_quartets = n_dontcares_div.getValue();

            while (iter1.hasNext()) {
                id_pos_dcares1 = iter1.next();
                id1 = (int) (id_pos_dcares1._1 >> 32);

                if (id1 >= n_v) // sequence is reverse
                    continue;

                pos1 = id_pos_dcares1._1.intValue();
                iter2 = W_list._2.iterator();

                while (iter2.hasNext()) {
                    id_pos_dcares2 = iter2.next();
                    id2 = (int) (id_pos_dcares2._1 >> 32);

                    if (id2 >= n_v) // sequence is reverse, restore true id
                        id2 -= n_v;

                    if (id2 <= id1)
                        continue;

                    pos2 = id_pos_dcares2._1.intValue();

                    score = nMismatches = 0;
                    threshBroken = false;

                    for (i = 0; i < n_quartets-1; i++) {
                        if (score + (n_quartets-1 - i) * 400 <= threshold.getValue()) {
                            threshBroken = true;
                            break;
                        }

                        score += Chiaromonte[0][id_pos_dcares1._2[i] & 0xFF][id_pos_dcares2._2[i] & 0xFF];
                        nMismatches += mismatches[0][id_pos_dcares1._2[i] & 0xFF][id_pos_dcares2._2[i] & 0xFF];
                    }

                    if (threshBroken) continue;

                    score += Chiaromonte[n_dontcares_mod.getValue()][id_pos_dcares1._2[i] & 0xFF][id_pos_dcares2._2[i] & 0xFF];
                    nMismatches += mismatches[n_dontcares_mod.getValue()][id_pos_dcares1._2[i] & 0xFF][id_pos_dcares2._2[i] & 0xFF];

                    if (score > threshold.getValue())
                        matches.mergeInt(new Match(id1, id2, pos1, pos2, nMismatches), score, Math::max);
                }
            }

            for (Match m : matches.keySet())
                mismatches_list.add(new Tuple2<>(new Tuple2<>(m.id1, m.id2), m.mismatch));

            return mismatches_list.iterator();
        });

        // <(id1, id2), [nMismatch]>  -->  <(id1, id2), distance>
        JavaPairRDD<Tuple2<Integer, Integer>, Double> ids_distances = mismatches_rdd.aggregateByKey(
                new Tuple2<>(0, 0),
                (t, mis) -> new Tuple2<>(t._1 + mis, t._2 + n_dontcares.getValue()),
                (t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2)
        ).mapValues(t -> -3.0 / 4 * Math.log(1 - 4.0 / 3 * t._1 / t._2));

        JavaPairRDD<Tuple2<String, String>, Double> names_distances = ids_distances.mapToPair(ids_distance -> {
            Tuple2<String, String> names = new Tuple2<>(id2key_bc.getValue().get(ids_distance._1._1), id2key_bc.getValue().get(ids_distance._1._2));

            return new Tuple2<>(names, ids_distance._2);
        });

        names_distances.saveAsTextFile("distance");

        System.out.println("Computed distance for " + input_path + " with num_slices=" + num_slices + ", l=" + pattern.length());

        spark.close();
    }
}