package Simulation;

import StorageLayer.DatabaseManager;
import StorageLayer.AuditRecord;
import javafx.animation.AnimationTimer;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.stage.Stage;
import swarm.SwarmNode;
import swarm.SwarmNode.NodeStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import StorageLayer.AuditRecord;
import StorageLayer.AuditRecord;

public class VisualizeSimulate extends Application {

    private static final int CANVAS_SIZE = 700;
    private static final double WORLD_SIZE = 100.0;
    private static final double SCALE = CANVAS_SIZE / WORLD_SIZE;

    private static final int NODE_COUNT = 3;
    private static final double COMMUNICATION_RANGE = 55.0;
    private static final double PLANE_ALTITUDE = 15.0;
    private static final String SWARM_ID = "SWARM_ALPHA";

    private final List<SwarmNode> swarm = new ArrayList<>();
    private final ScheduledExecutorService physicsExecutor = Executors.newScheduledThreadPool(3);
    private final AtomicBoolean node2Failed = new AtomicBoolean(false);
    private final AtomicBoolean node1Rerouted = new AtomicBoolean(false);

    private final DatabaseManager dbManager = new DatabaseManager();
    private final ObservableList<AuditRecord> tableData = FXCollections.observableArrayList();

    private Label telemetryLabel;

    @Override
    public void start(Stage primaryStage) {
        initSwarmPhysics();

        BorderPane root = new BorderPane();
        root.setStyle("-fx-background-color: #121318;");

        // 1. Radar Canvas (Left)
        Canvas canvas = new Canvas(CANVAS_SIZE, CANVAS_SIZE);
        GraphicsContext gc = canvas.getGraphicsContext2D();

        // 2. Live Database Audit Table (Right)
        VBox tableContainer = createDatabaseAuditPane();

        HBox centerPane = new HBox(15, canvas, tableContainer);
        centerPane.setPadding(new Insets(10));
        root.setCenter(centerPane);

        // 3. Bottom Telemetry HUD
        telemetryLabel = new Label("Initializing swarm telemetry...");
        telemetryLabel.setFont(Font.font("Monospaced", 13));
        telemetryLabel.setTextFill(Color.web("#d1d5db"));

        HBox bottomBar = new HBox(telemetryLabel);
        bottomBar.setPadding(new Insets(10));
        bottomBar.setStyle("-fx-background-color: #1a1b23; -fx-border-color: #2e303e; -fx-border-width: 1 0 0 0;");
        root.setBottom(bottomBar);

        // 60 FPS Canvas Render Loop
        AnimationTimer renderLoop = new AnimationTimer() {
            @Override
            public void handle(long now) {
                render(gc);
                updateTelemetryHUD();
            }
        };
        renderLoop.start();

        // 2Hz DB Poller to refresh table data without blocking JavaFX thread
        physicsExecutor.scheduleAtFixedRate(() -> {
            List<AuditRecord> latest = dbManager.fetchRecentRecords(20);
            Platform.runLater(() -> {
                tableData.setAll(latest);
            });
        }, 500, 500, TimeUnit.MILLISECONDS);

        Scene scene = new Scene(root, CANVAS_SIZE + 440, CANVAS_SIZE + 65);
        primaryStage.setTitle("SwarmNode - Live Autonomous Coverage & Tier 2 Audit Visualizer");
        primaryStage.setResizable(false);
        primaryStage.setScene(scene);
        primaryStage.setOnCloseRequest(e -> {
            physicsExecutor.shutdownNow();
            System.exit(0);
        });
        primaryStage.show();
    }

    private VBox createDatabaseAuditPane() {
        Label header = new Label("TIER 2 SQLITE PERSISTENCE AUDIT");
        header.setFont(Font.font("Monospaced", 13));
        header.setTextFill(Color.web("#93c5fd"));

        TableView<AuditRecord> table = new TableView<>(tableData);
        table.setPrefWidth(420);
        table.setPrefHeight(CANVAS_SIZE - 40);
        table.setStyle("-fx-background-color: #161822; -fx-text-fill: #e5e7eb; -fx-font-family: 'Monospaced';");

        TableColumn<AuditRecord, Integer> idCol = new TableColumn<>("ID");
        idCol.setCellValueFactory(new PropertyValueFactory<>("id"));
        idCol.setPrefWidth(50);

        TableColumn<AuditRecord, Integer> nodeCol = new TableColumn<>("Node");
        nodeCol.setCellValueFactory(new PropertyValueFactory<>("nodeId"));
        nodeCol.setPrefWidth(60);

        TableColumn<AuditRecord, Integer> tierCol = new TableColumn<>("Tier");
        tierCol.setCellValueFactory(new PropertyValueFactory<>("tier"));
        tierCol.setPrefWidth(55);

        TableColumn<AuditRecord, String> tsCol = new TableColumn<>("Timestamp");
        tsCol.setCellValueFactory(new PropertyValueFactory<>("timestamp"));
        tsCol.setPrefWidth(240);

        table.getColumns().addAll(idCol, nodeCol, tierCol, tsCol);

        VBox box = new VBox(10, header, table);
        box.setPadding(new Insets(10));
        box.setStyle("-fx-background-color: #1a1b24; -fx-border-color: #2e303e; -fx-border-width: 1; -fx-border-radius: 4;");
        return box;
    }

    private void initSwarmPhysics() {
        double[][] initPos = {
            {25.0, 25.0, PLANE_ALTITUDE},
            {75.0, 25.0, PLANE_ALTITUDE},
            {25.0, 75.0, PLANE_ALTITUDE}
        };

        double[][] velocities = {
            {1.2, 0.8, 0.0},
            {0.4, 1.1, 0.0},
            {0.9, -0.7, 0.0}
        };

        for (int i = 0; i < NODE_COUNT; i++) {
            SwarmNode node = new SwarmNode(
                "NODE_" + (i + 1),
                initPos[i],
                velocities[i],
                COMMUNICATION_RANGE,
                "DESP_11BYTE_BIN",
                SWARM_ID
            );
            swarm.add(node);
        }

        for (int i = 0; i < swarm.size(); i++) {
            for (int j = 0; j < swarm.size(); j++) {
                if (i != j) swarm.get(i).addNeighbor(swarm.get(j));
            }
        }

        // Kinematics and communication cycle (30Hz)
        physicsExecutor.scheduleAtFixedRate(() -> {
            for (SwarmNode node : swarm) {
                if ("NODE_1".equals(node.getNodeId()) && node1Rerouted.get()) {
                    double[] p = node.getPosition();
                    double targetX = 75.0;
                    double targetY = 30.0;
                    double dx = targetX - p[0];
                    double dy = targetY - p[1];
                    double dist = Math.sqrt(dx * dx + dy * dy);
                    if (dist > 2.0) {
                        p[0] += (dx / dist) * 2.2 * 0.033;
                        p[1] += (dy / dist) * 2.2 * 0.033;
                    }
                } else if (node.getStatus() != NodeStatus.MALFUNCTIONING) {
                    node.move(0.033);
                    clampAndBounce(node);
                }
                node.communicate();
            }
        }, 100, 33, TimeUnit.MILLISECONDS);

        // Fault Injection on Node 2 at t = 2.0s
        physicsExecutor.schedule(() -> {
            node2Failed.set(true);
            SwarmNode node2 = swarm.get(1);
            node2.setBatteryLevel(9.8);
            node2.setStatus(NodeStatus.MALFUNCTIONING);
            node2.communicate();
        }, 2000, TimeUnit.MILLISECONDS);

        // Re-tasking on Node 1 at t = 2.8s
        physicsExecutor.schedule(() -> {
            node1Rerouted.set(true);
        }, 2800, TimeUnit.MILLISECONDS);
    }

    private void clampAndBounce(SwarmNode node) {
        double[] pos = node.getPosition();
        if (pos[0] <= 8.0 || pos[0] >= 92.0) {
            pos[0] = Math.max(8.0, Math.min(92.0, pos[0]));
        }
        if (pos[1] <= 8.0 || pos[1] >= 92.0) {
            pos[1] = Math.max(8.0, Math.min(92.0, pos[1]));
        }
    }

    private void render(GraphicsContext gc) {
        gc.setFill(Color.web("#121318"));
        gc.fillRect(0, 0, CANVAS_SIZE, CANVAS_SIZE);

        // 10m grid
        gc.setStroke(Color.web("#1e222d"));
        gc.setLineWidth(1.0);
        for (int i = 0; i <= WORLD_SIZE; i += 10) {
            double coord = i * SCALE;
            gc.strokeLine(coord, 0, coord, CANVAS_SIZE);
            gc.strokeLine(0, coord, CANVAS_SIZE, coord);
        }

        // Sector Quadrants
        gc.setStroke(Color.web("#2d3345"));
        gc.setLineDashes(6.0);
        gc.strokeLine(CANVAS_SIZE / 2.0, 0, CANVAS_SIZE / 2.0, CANVAS_SIZE);
        gc.strokeLine(0, CANVAS_SIZE / 2.0, CANVAS_SIZE, CANVAS_SIZE / 2.0);
        gc.setLineDashes(null);

        gc.setFont(Font.font("Monospaced", 12));
        gc.setFill(Color.web("#4b5563"));
        gc.fillText("QUAD A [Node 1 Patrol]", 20, 30);
        gc.fillText(node2Failed.get() ? "QUAD B [ABANDONED - GAP]" : "QUAD B [Node 2 Patrol]", CANVAS_SIZE / 2.0 + 20, 30);
        gc.fillText("QUAD C [Node 3 Patrol]", 20, CANVAS_SIZE / 2.0 + 30);
        gc.fillText("QUAD D [Reserve]", CANVAS_SIZE / 2.0 + 20, CANVAS_SIZE / 2.0 + 30);

        // Links
        gc.setLineWidth(1.5);
        for (int i = 0; i < swarm.size(); i++) {
            for (int j = i + 1; j < swarm.size(); j++) {
                SwarmNode n1 = swarm.get(i);
                SwarmNode n2 = swarm.get(j);
                double[] p1 = n1.getPosition();
                double[] p2 = n2.getPosition();

                double dx = p1[0] - p2[0];
                double dy = p1[1] - p2[1];
                double dist = Math.sqrt(dx * dx + dy * dy);

                if (dist <= COMMUNICATION_RANGE) {
                    if (n1.getStatus() == NodeStatus.MALFUNCTIONING || n2.getStatus() == NodeStatus.MALFUNCTIONING) {
                        gc.setStroke(Color.web("#ef4444", 0.7));
                    } else {
                        gc.setStroke(Color.web("#10b981", 0.35));
                    }
                    gc.strokeLine(p1[0] * SCALE, p1[1] * SCALE, p2[0] * SCALE, p2[1] * SCALE);
                }
            }
        }

        // Nodes
        for (SwarmNode node : swarm) {
            double[] pos = node.getPosition();
            double screenX = pos[0] * SCALE;
            double screenY = pos[1] * SCALE;

            double r = COMMUNICATION_RANGE * SCALE;
            gc.setStroke(Color.web("#3b82f6", 0.12));
            gc.strokeOval(screenX - r, screenY - r, r * 2, r * 2);

            if (node.getStatus() == NodeStatus.MALFUNCTIONING) {
                gc.setFill(Color.web("#ef4444"));
            } else if ("NODE_1".equals(node.getNodeId()) && node1Rerouted.get()) {
                gc.setFill(Color.web("#f59e0b"));
            } else {
                gc.setFill(Color.web("#10b981"));
            }

            gc.fillOval(screenX - 7, screenY - 7, 14, 14);

            gc.setFill(Color.web("#f3f4f6"));
            gc.setFont(Font.font("Monospaced", 11));
            gc.fillText(node.getNodeId(), screenX + 10, screenY - 5);
        }
    }

    private void updateTelemetryHUD() {
        StringBuilder sb = new StringBuilder();
        for (SwarmNode n : swarm) {
            sb.append(String.format("[%s: %s | Batt: %.1f%%]   ",
                n.getNodeId(),
                n.getStatus(),
                n.getBatteryLevel()
            ));
        }
        if (node1Rerouted.get()) {
            sb.append(">>> [RETASKED: NODE_1 COVERING QUAD B]");
        } else if (node2Failed.get()) {
            sb.append(">>> [EVENT: NODE_2 FAILURE (TIER 2 DB WRITE)]");
        }
        telemetryLabel.setText(sb.toString());
    }

    public static void main(String[] args) {
        launch(args);
    }
}