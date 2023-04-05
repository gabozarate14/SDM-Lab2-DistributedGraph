package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Exercise_4 {

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) throws IOException {
		// Create Vertices
		//String content = new String(Files.readAllBytes(Paths.get("src/main/resources/wiki-vertices.txt")));
		List<String> content_vertex = Files.readAllLines(Paths.get("src/main/resources/wiki-vertices.txt"));
		java.util.List<Row> vertices_list = new ArrayList<Row>();

		for(int i = 0; i<content_vertex.size(); i++){
			String[] vertex = content_vertex.get(i).split("\\s+", 2);
			vertices_list.add(RowFactory.create(vertex[0],Arrays.asList(vertex).subList(1, vertex.length).get(0)));
			//vertices_list.add(RowFactory.create(vertex[0],vertex));
		}

		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);

		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);
		// Create Edges
		List<String> content_edges = Files.readAllLines(Paths.get("src/main/resources/wiki-edges.txt"));
		java.util.List<Row> edges_list = new ArrayList<Row>();

		for(int i = 0; i<content_edges.size(); i++){
			String[] edge = content_edges.get(i).split("\\s+", 2);
			edges_list.add(RowFactory.create(edge[0], edge[edge.length-1]));
		}

		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);

		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

		GraphFrame gf = GraphFrame.apply(vertices,edges);

		System.out.println(gf);
		gf.edges().show();
		gf.vertices().show();


		// RUN Page Rank
		gf.pageRank()
				.maxIter(10)
				.resetProbability(0.8)
				.run()
				.vertices().orderBy("pagerank")
				.show(10);
	}
}