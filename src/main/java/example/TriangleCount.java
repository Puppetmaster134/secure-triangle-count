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
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Relationship;

import org.neo4j.graphdb.traversal.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.*;
import java.util.function.Consumer;

import java.lang.Math;
import java.lang.IllegalArgumentException;

/**
 *
 * @author brian
 */
public class TriangleCount {

    static final Label PERSON = Label.label("Person");

    @Context
    public GraphDatabaseService db;
    
    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    /**
     * Regular triangle counting algorithm
     * 
     * @return  A triangle count instance with the number of triangles for each vertex in the (sub) graph
     */
    @Procedure(value = "example.triangleCount", mode=Mode.WRITE)
    @Description("Count triangles.")
    public Stream<NodeTriangleCount> triangleCount() {
        List<Node> nodes = db.beginTx().findNodes(PERSON).stream().collect(Collectors.toList());
        int nodeTriCount[] = countAllVertexTriangles();

        ArrayList<NodeTriangleCount> finalCounts = new ArrayList<NodeTriangleCount>();
        for (int i = 0; i < nodeTriCount.length; i++)
        {
            finalCounts.add(new NodeTriangleCount((long) i, (long) nodeTriCount[i]));
        }

        return finalCounts.stream();
    }

    @Procedure(value = "example.triangleHistogram", mode=Mode.WRITE)
    @Description("Create a triangle count histogram aggregation.")
    public Stream<PerturbedValue> triangleHistogram() {
        List<Long> counts = triangleCount()
            .map(count -> count.triangleCount)
            .collect(Collectors.toList());
        
        ArrayList<PerturbedValue> perturbedValues = new ArrayList<PerturbedValue>();

        IntStream.rangeClosed(0, Collections.max(counts).intValue()).forEachOrdered(step -> {
            int numVertices = Collections.frequency(counts, (long) step);
            perturbedValues.add(new PerturbedValue((long) step, (double) numVertices));
        });

        
        return perturbedValues.stream();
    }

    private int TriangleCountByNodeId(long nodeId)
    {       
        Node node = db.beginTx().getNodeById(nodeId);
        int triangleCount = 0;
        
        List<Long> triangleCandidateIds = StreamSupport
            .stream(node.getRelationships().spliterator(),false)
            .map(rel -> rel.getOtherNodeId(node.getId()))
            .collect(Collectors.toList());

        for(Relationship rel : node.getRelationships())
        {
            Node neighbor = rel.getOtherNode(node);
            List<Relationship> thirdEdges = StreamSupport
                .stream(neighbor.getRelationships().spliterator(),false)
                .filter(thirdEdge -> triangleCandidateIds.contains(thirdEdge.getOtherNode(neighbor).getId()))
                .filter(thirdEdge -> thirdEdge.getOtherNode(neighbor).getId() > neighbor.getId())
                .collect(Collectors.toList());
            
            triangleCount += thirdEdges.size();
        }

        return triangleCount;
    }

    private int EdgeTriangleCount(long firstNodeId, long secondNodeId)
    {
        Node firstNode = db.beginTx().getNodeById(firstNodeId);
        Node secondNode = db.beginTx().getNodeById(secondNodeId);
        
        List<Long> firstNodeNeighbors = StreamSupport
            .stream(firstNode.getRelationships().spliterator(),false)
            .map(rel -> rel.getOtherNodeId(firstNodeId))
            .collect(Collectors.toList());

        List<Long> secondNodeNeighbors = StreamSupport
            .stream(secondNode.getRelationships().spliterator(),false)
            .map(rel -> rel.getOtherNodeId(secondNodeId))
            .collect(Collectors.toList());
        
        Set<Long> result = firstNodeNeighbors.stream()
            .distinct()
            .filter(secondNodeNeighbors::contains)
            .collect(Collectors.toSet());

        return result.size();
    }

    private boolean DeleteEdge(long originId, long targetId)
    {
        final long deletionCandidateId = targetId;
                    
        try (Transaction tx = db.beginTx()) {
            Node originVertex = tx.getNodeById(originId);

            for (Relationship r : originVertex.getRelationships()) {
                if(r.getOtherNodeId(originVertex.getId()) == targetId)
                {
                    r.delete();
                }
            }
            tx.commit();
        }

        return true;
    }

    private int[] countAllVertexTriangles()
    {
        List<Node> nodes = db.beginTx().findNodes(PERSON).stream().collect(Collectors.toList());

        int nodeTriCount[] = new int[nodes.size()];
        for(Node node : nodes)
        {
            nodeTriCount[(int) node.getId()] = TriangleCountByNodeId(node.getId());
        }

        return nodeTriCount;
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
    public static class NodeTriangleCount {
        public Long nodeId;
        public Long triangleCount;

        public NodeTriangleCount(Long nodeId, Long triangleCount)
        {
            this.nodeId = nodeId;
            this.triangleCount = triangleCount;
        }
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
    public static class PerturbedValue {
        public Long step;
        public Double perturbedValue;
        
        public PerturbedValue(long step, Double count)
        {
            this.step = step;
            this.perturbedValue = count;
        }

        public String toString()
        {
            return (this.step + " -- " + this.perturbedValue);
        }
    }
}
