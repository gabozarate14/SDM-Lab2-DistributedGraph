package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long, Tuple2<Integer,List<Object>>, Tuple2<Integer,List<Object>>, Tuple2<Integer,List<Object>>> implements Serializable {
        @Override
        public Tuple2<Integer,List<Object>> apply(Long vertexID, Tuple2<Integer,List<Object>> vertexValue, Tuple2<Integer,List<Object>> message) {

            if (message._1 == Integer.MAX_VALUE) { // superstep 0
                return vertexValue;
            } else { // superstep > 0
                Integer distance = Math.min(vertexValue._1, message._1);
                return new Tuple2<>(distance, message._2);
            }
        }
    }
    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Integer,List<Object>>, Integer>, Iterator<Tuple2<Object, Tuple2<Integer,List<Object>>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Integer,List<Object>>>> apply(EdgeTriplet<Tuple2<Integer,List<Object>>, Integer> triplet) {
            Tuple2<Object, Tuple2<Integer,List<Object>>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, Tuple2<Integer,List<Object>>> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2._1.equals(Integer.MAX_VALUE)) {
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Tuple2<Integer,List<Object>>>>().iterator()).asScala();
            } else if (sourceVertex._2._1 + triplet.attr() < dstVertex._2._1) {
                // propagate updated minimum distance
                Integer dist = sourceVertex._2._1 + triplet.attr() ;
                sourceVertex._2._2.add(triplet.dstId());
                Tuple2<Integer,List<Object>>  msg = new Tuple2<>(dist, sourceVertex._2._2);
                Tuple2<Object, Tuple2<Integer,List<Object>>> msgNode = new Tuple2<>(triplet.dstId(),msg);
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(msgNode).iterator()).asScala();
            } else {
                // do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Tuple2<Integer,List<Object>>>>().iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Tuple2<Integer,List<Object>>,Tuple2<Integer,List<Object>>,Tuple2<Integer,List<Object>>> implements Serializable {
        @Override
        public Tuple2<Integer,List<Object>> apply(Tuple2<Integer,List<Object>> o, Tuple2<Integer,List<Object>> o2) {
            return null;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();


        List<Tuple2<Object, Tuple2<Integer,List<Object>>>> vertices = Lists.newArrayList(
                new Tuple2<Object, Tuple2<Integer,List<Object>>>(1l, new Tuple2<>(0, new ArrayList<>())),
                new Tuple2<Object, Tuple2<Integer,List<Object>>>(2l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<>())),
                new Tuple2<Object, Tuple2<Integer,List<Object>>>(3l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<>())),
                new Tuple2<Object, Tuple2<Integer,List<Object>>>(4l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<>())),
                new Tuple2<Object, Tuple2<Integer,List<Object>>>(5l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<>())),
                new Tuple2<Object, Tuple2<Integer,List<Object>>>(6l, new Tuple2<>(Integer.MAX_VALUE, new ArrayList<>()))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l, 2l, 4), // A --> B (4)
                new Edge<Integer>(1l, 3l, 2), // A --> C (2)
                new Edge<Integer>(2l, 3l, 5), // B --> C (5)
                new Edge<Integer>(2l, 4l, 10), // B --> D (10)
                new Edge<Integer>(3l, 5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object, Tuple2<Integer,List<Object>>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Tuple2<Integer,List<Object>>, Integer> G = Graph.apply(
                verticesRDD.rdd(),
                edgesRDD.rdd(),
                new Tuple2<>(1, new ArrayList<>()),
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
        
        ops.pregel(new Tuple2<>(Integer.MAX_VALUE, new ArrayList<>()),
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        ClassTag$.MODULE$.apply(Tuple2.class))
                .vertices()
                .toJavaRDD()
                .foreach(v -> {
                        Tuple2<Object, Tuple2<Integer, List<Object>>> vertex = (Tuple2< Object, Tuple2<Integer, List<Object>>>) v;
                    List<Object> path = vertex._2._2.stream()
                            .map(labels::get)
                            .collect(Collectors.toList());
                    //Adding the first node to the path:
                    path.add(0,"A");
                    System.out.println("Minimum cost to get from " + labels.get(1l) + " to " + labels.get(vertex._1) + " is " + path + " with cost " + vertex._2._1);
                });

    }
}
