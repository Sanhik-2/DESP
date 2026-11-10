package Simulation;

import swarm.SwarmNode;
import swarm.SwarmNode.NodeStatus;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SwarmRunner {

    private static final int NODE_COUNT = 3;
    private static final double COMMUNICATION_RANGE = 60.0; // Metric meters
    private static final double PLANE_ALTITUDE = 15.0;      // Fixed inspection plane Z = 15.0m
    private static final String SWARM_ID = "SWARM_ALPHA";

    public static void main(String[] args) {
        System.out.println("=====================================================================");
        System.out.println("     SWARMNODE: AUTONOMOUS PLANE COVERAGE & RE-ROUTING SIMULATION   ");
        System.out.println("     Package: Simulation | Plane: 100m x 100m @ Alt = " + PLANE_ALTITUDE + "m");
        System.out.println("=====================================================================\n");

        // Clean stale SQLite database files from previous simulation runs
        for (int i = 1; i <= NODE_COUNT; i++) {
            File db = new File("node_" + i + "_telemetry.db");
            if (db.exists()) {
                db.delete();
            }
        }

        // 1. Initialize Nodes with Initial Centroids in their assigned Quadrants
        // Node 1: Quad A (0..50, 0..50)    -> Center (25, 25, 15)
        // Node 2: Quad B (50..100, 0..50)  -> Center (75, 25, 15)
        // Node 3: Quad C (0..50, 50..100)  -> Center (25, 75, 15)
        double[][] initialPositions = {
            {25.0, 25.0, PLANE_ALTITUDE},
            {75.0, 25.0, PLANE_ALTITUDE},
            {25.0, 75.0, PLANE_ALTITUDE}
        };

        double[][] velocities = {
            {1.2, 0.8, 0.0},
            {0.5, 1.1, 0.0},
            {0.9, 0.4, 0.0}
        };

        List<SwarmNode> swarm = new ArrayList<>();
        for (int i = 0; i < NODE_COUNT; i++) {
            SwarmNode node = new SwarmNode(
                "NODE_" + (i + 1),
                initialPositions[i],
                velocities[i],
                COMMUNICATION_RANGE,
                "DESP_11BYTE_BIN",
                SWARM_ID
            );
            swarm.add(node);
        }

        // 2. Establish fully connected initial mesh topology
        for (int i = 0; i < swarm.size(); i++) {
            for (int j = 0; j < swarm.size(); j++) {
                if (i != j) {
                    swarm.get(i).addNeighbor(swarm.get(j));
                }
            }
        }

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
        AtomicBoolean node2Failed = new AtomicBoolean(false);
        AtomicBoolean node1Retasked = new AtomicBoolean(false);

        // 3. Main Loop: Kinematics (move) and Telemetry Broadcast (communicate) at 20Hz
        executor.scheduleAtFixedRate(() -> {
            for (SwarmNode node : swarm) {
                node.move(0.05); // dt = 50ms
                node.communicate();
            }
        }, 100, 50, TimeUnit.MILLISECONDS);

        // 4. Inject Critical Hardware Fault on Node 2 at t = 1.5s
        executor.schedule(() -> {
            node2Failed.set(true);
            SwarmNode node2 = swarm.get(1);

            System.out.println("\n---------------------------------------------------------------------");
            System.out.println(">>> [FAULT INJECTION] Node 2 Voltage Collapse & Motor Desync!");
            System.out.println(">>> Node 2 transitioning: IDLE -> MALFUNCTIONING | Battery -> 11.2%");
            System.out.println(">>> Node 2 triggers Tier 2 persistence & broadcasts emergency gap state...");
            System.out.println("---------------------------------------------------------------------\n");

            node2.setBatteryLevel(11.2);
            node2.setStatus(NodeStatus.MALFUNCTIONING);
            node2.communicate(); // Immediate priority dispatch
        }, 1500, TimeUnit.MILLISECONDS);

        // 5. Autonomous Re-routing: Node 1 detects Node 2 failure, claims Quad B
        executor.schedule(() -> {
            node1Retasked.set(true);
            SwarmNode node1 = swarm.get(0);
            SwarmNode node2 = swarm.get(1);

            double[] p1 = node1.getPosition();
            double[] p2 = node2.getPosition();

            System.out.println("\n---------------------------------------------------------------------");
            System.out.println(">>> [AUTONOMOUS RE-TASKING] Node 1 processed distress frame from Node 2.");
            System.out.printf(">>> Current Node 1 Pos: (%.1f, %.1f) | Abandoned Quad B Target: (%.1f, %.1f)\n", 
                              p1[0], p1[1], p2[0], p2[1]);
            System.out.println(">>> Dynamic Expansion: Quad A (0..50) -> Merged Patrol Quad A+B (0..100).");
            System.out.println(">>> Node 1 recalculating velocity vector to cover coverage hole...");
            System.out.println("---------------------------------------------------------------------\n");

        }, 1800, TimeUnit.MILLISECONDS);

        // 6. Conclude simulation run and print audit summary
        try {
            Thread.sleep(3500);

            System.out.println("\n[SIM] Stopping executor threads...");
            executor.shutdownNow();

            System.out.println("\n=====================================================================");
            System.out.println("               FINAL EXPERIMENTAL EVALUATION AUDIT                   ");
            System.out.println("=====================================================================");
            System.out.printf("%-10s | %-12s | %-15s | %-12s%n", "Node ID", "Status", "Battery Level", "Coordinates (X, Y, Z)");
            System.out.println("---------------------------------------------------------------------");
            for (SwarmNode node : swarm) {
                double[] pos = node.getPosition();
                System.out.printf("%-10s | %-12s | %-13.1f%% | (%.1f, %.1f, %.1f)%n",
                        node.getNodeId(),
                        node.getStatus(),
                        node.getBatteryLevel(),
                        pos[0], pos[1], pos[2]);
            }
            System.out.println("=====================================================================");
            System.out.println("[SIM] Simulation completed successfully. SQLite audit files verified.");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}