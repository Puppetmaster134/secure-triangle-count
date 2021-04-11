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

import org.neo4j.graphdb.traversal.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.*;
import java.util.function.Consumer;

import java.util.Arrays;
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


        Stream<Node> vertices = db.beginTx().findNodes(PERSON).stream();

        List<Node> nodes = vertices.collect(Collectors.toList());
        System.out.println(nodes.size() + " nodes.");
        int nodeTriCount[] = new int[nodes.size()];
        int edgeTriCount[][] = new int[nodes.size()][nodes.size()];

        for(Node node : nodes)
        {
            long nodeId = node.getId();
            ArrayList<Long> linkNodes = new ArrayList<Long>();
            ArrayList<Node> neighbors = new ArrayList<Node>();

            for(Relationship r : node.getRelationships())
            {
                linkNodes.add(r.getOtherNodeId(nodeId));
                neighbors.add(r.getOtherNode(node));
            }

            int triangleCount = 0;
            for(Node neighbor : neighbors){
                List<Long> triangleCandidateIds = linkNodes.stream()
                .collect(Collectors.toList());

                List<Relationship> thirdEdges = StreamSupport.stream(neighbor.getRelationships().spliterator(),false)
                    .filter(rel -> triangleCandidateIds.contains(rel.getOtherNodeId(neighbor.getId())))
                    .collect(Collectors.toList());
                
                
                //How many triangles the edge between node(i)->neighbor(j) participates in
                edgeTriCount[(int) nodeId][(int) neighbor.getId()] = thirdEdges.size();

                //Add to total triangle count for node(i)
                triangleCount += thirdEdges.stream().filter(rel -> rel.getOtherNodeId(neighbor.getId()) > neighbor.getId()).collect(Collectors.toList()).size();
            }

            nodeTriCount[(int)nodeId] = triangleCount;
        }

        //System.out.println(Arrays.toString(nodeTriCount));
        //System.out.println(Arrays.deepToString(edgeTriCount));

        for(Node node : nodes)
        {
            int nodeId = (int) node.getId();

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
                        // Line 15

                    }
                }




                //Update Triangle Count
                nodeTriCount[nodeId] = TriangleCountByNodeId(node.getId());
                break;
            }
        }

        return Stream.of(new TriangleCount(69));
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
