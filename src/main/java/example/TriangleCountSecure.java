/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package example;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.Stream;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author brian
 */
public class TriangleCountSecure {

    static final Label PERSON = Label.label("Person");

    @Context
    public GraphDatabaseService db;
        // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    /**
     * Best Adaption algorithm for triangle counting queries
     * Differentially Private Triangle Counting in Large Graphs, Ding et al.,2021
     * Algorithm 2 in the paper
     * 
     * @param lambda  The upper bound to impose on the subgraph
     * @return  A triangle count instance with the number of triangles for each vertex in the (sub) graph
     */
    @Procedure(value = "example.triangleCountSecure")
    @Description("Securely count triangles.")
    public Stream<TriangleCount> triangleCountSecure(@Name("lambda") Number lambda) {
        
        Consumer<Node> countNodeTriangles = node -> {

            ArrayList<Relationship> rels = new ArrayList<Relationship>();
            for(Relationship r : node.getRelationships())
            {
                rels.add(r);
            }

            //Degree of node
            System.out.println(rels.size());



            //Query: How many triangles v_i links to?
            //sequence of nodes linked to v_i
        };

        Stream<Node> vertices = db.beginTx().findNodes(PERSON).stream();
        vertices.forEach(countNodeTriangles);


        List<Number> testcity = new ArrayList<Number>();
        testcity.add(69);
        return Stream.of(new TriangleCount(69));
    }


    /**
     * This is the output record for our search procedure. All procedures
     * that return results return them as a Stream of Records, where the
     * records are defined like this one - customized to fit what the procedure
     * is returning.
     * <p>
     * These classes can only have public non-final fields, and the fields must
     * be one of the following types:
     *
     * <ul>
     *     <li>{@link String}</li>
     *     <li>{@link Long} or {@code long}</li>
     *     <li>{@link Double} or {@code double}</li>
     *     <li>{@link Number}</li>
     *     <li>{@link Boolean} or {@code boolean}</li>
     *     <li>{@link Node}</li>
     *     <li>{@link org.neo4j.graphdb.Relationship}</li>
     *     <li>{@link org.neo4j.graphdb.Path}</li>
     *     <li>{@link Map} with key {@link String} and value {@link Object}</li>
     *     <li>{@link List} of elements of any valid field type, including {@link List}</li>
     *     <li>{@link Object}, meaning any of the valid field types</li>
     * </ul>
     */
    public static class TriangleCount {
        // These records contain two lists of distinct relationship types going in and out of a Node.
        public Number numTriangles;

        public TriangleCount(Number triangleCount) {
            this.numTriangles = triangleCount;
        }
    }
}
