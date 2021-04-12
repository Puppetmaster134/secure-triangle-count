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

import org.apache.commons.math3.distribution.LaplaceDistribution;

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
    @Procedure(value = "example.triangleCountSecure", mode=Mode.WRITE)
    @Description("Securely count triangles.")
    public Stream<NodeTriangleCount> triangleCountSecure(@Name("lambda") Number lambda) {
        List<Node> nodes = db.beginTx().findNodes(PERSON).stream().collect(Collectors.toList());
        int nodeTriCount[] = countAllVertexTriangles();
        int edgeTriCount[][] = countAllEdgeTriangles();

        //System.out.println(Arrays.toString(nodeTriCount));
        //System.out.println(Arrays.deepToString(edgeTriCount));

        for(Node node : nodes)
        {
            final int nodeId = (int) node.getId();

            while (nodeTriCount[nodeId] > lambda.intValue())
            {
                int temp = 0;
                long k = 0;

                for(Relationship rel : node.getRelationships())
                {
                    Node neighbor = rel.getOtherNode(node);
                    int neighborTriCount = TriangleCountByNodeId(neighbor.getId());

                    if (neighborTriCount > temp)
                    {
                        temp = neighborTriCount;
                        k = neighbor.getId();
                    }
                }

                if(temp < lambda.intValue())
                {
                    temp = nodeTriCount[nodeId] - lambda.intValue();
                    k = 0;
                    int minTemp = Integer.MAX_VALUE - 1;


                    for(Relationship rel : node.getRelationships())
                    {
                        Node neighbor = rel.getOtherNode(node);
                        int diff = Math.abs(temp - edgeTriCount[(int) nodeId][(int) neighbor.getId()]);

                        //Line 16
                        // Note: I used '<' instead of '>'. I think '>' is a mistake in the paper since minTemp is set to INT MAX
                        if (diff < minTemp)
                        {
                            minTemp = diff;
                            k = neighbor.getId();
                        }
                    }
                }

                //Delete edge v_i -> v_k
                try
                {
                    boolean success = DeleteEdge(nodeId, k);
                }
                catch(IllegalArgumentException e) { System.err.println(e); }

                //Update Triangle Count
                nodeTriCount[nodeId] = TriangleCountByNodeId(node.getId());
                edgeTriCount = countAllEdgeTriangles();
            }

            nodeTriCount = countAllVertexTriangles();
        }

        //System.out.println(Arrays.toString(nodeTriCount));
        //System.out.println(Arrays.deepToString(edgeTriCount));

        ArrayList<NodeTriangleCount> finalPerturbedCounts = new ArrayList<NodeTriangleCount>();
        for (int i = 0; i < nodeTriCount.length; i++)
        {
            finalPerturbedCounts.add(new NodeTriangleCount((long) i, (long) nodeTriCount[i]));
        }

        return finalPerturbedCounts.stream();
    }

    @Procedure(value = "example.triangleHistogramSecure", mode=Mode.WRITE)
    @Description("Securely count triangles.")
    public Stream<PerturbedValue> TriangleHistogramSecure(@Name("lambda") Number lambda, @Name("epsilon") Double epsilon) {
        List<Long> counts = triangleCountSecure(lambda)
            .map(count -> count.triangleCount)
            .collect(Collectors.toList());
        
        ArrayList<PerturbedValue> perturbedValues = new ArrayList<PerturbedValue>();

        Double distributionWidth = (( 4 * lambda.doubleValue() + 1) ) / epsilon;
        LaplaceDistribution lap = new LaplaceDistribution(0, distributionWidth);

        IntStream.rangeClosed(0, lambda.intValue()).forEachOrdered(step -> {
            int numVertices = Collections.frequency(counts, (long) step);
            perturbedValues.add(new PerturbedValue((long) step, (double) numVertices + lap.sample()));
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

    private int[][] countAllEdgeTriangles()
    {        
        List<Node> nodes = db.beginTx().findNodes(PERSON).stream().collect(Collectors.toList());
        int edgeTriCount[][] = new int[nodes.size()][nodes.size()];
        for(Node node : nodes)
        {        
            for(Relationship r : node.getRelationships())
            {
                long neighborId = r.getOtherNodeId(node.getId());
                edgeTriCount[(int) node.getId()][(int) neighborId] = EdgeTriangleCount(node.getId(), neighborId);
            }
        }

        return edgeTriCount;
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
