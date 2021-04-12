package example;

import org.junit.jupiter.api.*;
import org.neo4j.driver.*;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.*;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TriangleCountSecureTests {

    private static final Config driverConfig = Config.builder().withoutEncryption().build();
    private static Driver driver;
    private Neo4j embeddedDatabaseServer;

    @BeforeAll
    void initializeNeo4j() throws IOException {
        var sw = new StringWriter();
        try (var in = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/friend.cypher")))) {
            in.transferTo(sw);
            sw.flush();
        }

        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
            .withProcedure(TriangleCountSecure.class)
            .withFixture(sw.toString())
            .build();
    }


    @Test
    public void numTrianglesLessThanLambda() {
        try(
                Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()
            ) {

                int lambda = 2;
                //String formattedTestQuery = "CALL example.triangleCountSecure(3) YIELD numTriangles RETURN numTriangles";
                String formattedTestQuery = String.format("CALL example.triangleCountSecure(%d)",lambda);
                Record record = session.run(formattedTestQuery).single();
                
                List<Integer> values = new ArrayList<Integer>(record.values().get(0).asList(Values.ofInteger(),new ArrayList<Integer>()));

                System.out.println("Max Value: " + Collections.max(values));
                assertThat(Collections.max(values) <= lambda);
        }
    }


}
